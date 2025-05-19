const express = require('express');
const router = express.Router();
const db = require('../db');

// Hàm làm tròn 2 số thập phân
function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

// ========================
// 1. Stores Cube API
// ========================
router.get('/stores-cube', async (req, res) => {
  try {
    // 1. Lấy danh sách sản phẩm
    const [products] = await db.query(`
      SELECT Item_key, CONCAT('PID_', LPAD(Item_key, 2, '0')) AS col
      FROM Product_Dim
      ORDER BY Item_key
    `);

    const productCols = products.map(p => p.col);

    // 2. Lấy dữ liệu City + Store + Quantity
    const [rows] = await db.query(`
      SELECT 
        c.City_name AS city,
        sd.Store_key AS store_key,
        CONCAT('SID_', sd.Store_key) AS store_id,
        sf.Item_key,
        SUM(sf.Quantity) AS quantity
      FROM Stores_Fact sf
      JOIN Store_Dim sd ON sf.Store_key = sd.Store_key
      JOIN City_Dim c ON sd.City_key = c.City_key
      GROUP BY c.City_name, sd.Store_key, sf.Item_key
      ORDER BY c.City_name, sd.Store_key, sf.Item_key
    `);

    // 3. Gom nhóm theo City và Store để tạo dữ liệu phân cấp
    const cityMap = {};
    rows.forEach(row => {
      if (!cityMap[row.city]) cityMap[row.city] = { store: row.city, key: 'city-' + row.city, children: {}, _totals: {} };
      const city = cityMap[row.city];

      if (!city.children[row.store_id]) city.children[row.store_id] = { store: row.store_id, key: `city-${row.city}-store-${row.store_id}` };

      const col = 'PID_' + String(row.Item_key).padStart(2, '0');
      city.children[row.store_id][col] = Number(row.quantity);

      city._totals[col] = (city._totals[col] || 0) + Number(row.quantity);
    });

    // 4. Build lại thành mảng phân cấp cho frontend
    const data = [];
    Object.values(cityMap).forEach(city => {
      let cityTotal = 0;
      const storeArr = Object.values(city.children).map(store => {
        let storeTotal = 0;
        productCols.forEach(col => {
          store[col] = store[col] || 0;
          storeTotal += store[col];
        });
        store.Total_P = storeTotal;
        return store;
      });
      city.children = storeArr.length > 0 ? storeArr : undefined;

      productCols.forEach(col => {
        city[col] = city._totals[col] || 0;
        cityTotal += city[col];
      });
      city.Total_P = cityTotal;
      delete city._totals;
      data.push(city);
    });

    const columns = [
      { title: 'Store', dataIndex: 'store', key: 'store', width: 200 },
      ...productCols.map(col => ({
        title: col,
        dataIndex: col,
        key: col,
        align: 'right'
      })),
      { title: 'Total_P', dataIndex: 'Total_P', key: 'Total_P', align: 'right' }
    ];

    res.json({ columns, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ========================
// 2. Sales Cube API
// ========================

// ===============================
// API OLAP động cho drilldown, rollup, slice, dice
// ===============================

router.post('/sales-cube-dice', async (req, res) => {
  try {
    const {
      itemKeys = [],
      cityKeys = [],
      customerTypes = [],
      customerKeys = [],
      years = [],
      quarters = [],
      months = [],
    } = req.body;

    // ==> LẤY CHÍNH XÁC CÁC TRƯỜNG FILTER ĐƯỢC CHỌN <==
    const groupFields = [];
    const selects = [];

    // PRODUCT
    if (itemKeys && itemKeys.length > 0) {
      groupFields.push('p.Description');
      selects.push('p.Description AS PRODUCT');
    }
    // CITY
    if (cityKeys && cityKeys.length > 0) {
      groupFields.push('c.City_name');
      selects.push('c.City_name AS CITY');
    }
    // CUSTOMERTYPE
    if (customerTypes && customerTypes.length > 0) {
      groupFields.push('cd.Type_name');
      selects.push('cd.Type_name AS CUSTOMERTYPE');
    }
    // YEAR
    if (years && years.length > 0) {
      groupFields.push('t.Year');
      selects.push('t.Year AS YEAR');
    }
    // QUARTER
    if (quarters && quarters.length > 0) {
      groupFields.push('t.Quarter');
      selects.push('t.Quarter AS QUARTER');
    }
    // MONTH
    if (months && months.length > 0) {
      groupFields.push('t.Month');
      selects.push('t.Month AS MONTH');
    }

    // Luôn có 2 trường đo lường
    selects.push('SUM(sf.Units_sold) AS UNIT');
    selects.push('SUM(sf.Dollars_sold) AS DOLLAR');

    // WHERE động
    const wheres = [];
    if (itemKeys.length > 0) wheres.push(`sf.Item_key IN (${itemKeys.map(Number).join(',')})`);
    if (cityKeys.length > 0) wheres.push(`sf.City_key IN (${cityKeys.map(Number).join(',')})`);
    if (customerTypes.length > 0) wheres.push(`cd.Type_name IN (${customerTypes.map(t => `'${t}'`).join(',')})`);
    if (customerKeys.length > 0) wheres.push(`sf.Customer_key IN (${customerKeys.map(Number).join(',')})`);
    if (years.length > 0) wheres.push(`t.Year IN (${years.map(Number).join(',')})`);
    if (quarters.length > 0) wheres.push(`t.Quarter IN (${quarters.map(Number).join(',')})`);
    if (months.length > 0) wheres.push(`t.Month IN (${months.map(Number).join(',')})`);

    const whereSQL = wheres.length ? `WHERE ${wheres.join(' AND ')}` : '';
    const groupBySQL = groupFields.length ? `GROUP BY ${groupFields.join(', ')}` : '';
    const orderBySQL = groupFields.length ? `ORDER BY ${groupFields.join(', ')}` : '';

    const [rows] = await db.query(`
      SELECT ${selects.join(', ')}
      FROM Sales_Fact sf
      JOIN Product_Dim p ON sf.Item_key = p.Item_key
      JOIN City_Dim c ON sf.City_key = c.City_key
      JOIN Time_Dim t ON sf.Time_key = t.Time_key
      JOIN Customer_Dim cd ON sf.Customer_key = cd.Customer_key
      ${whereSQL}
      ${groupBySQL}
      ${orderBySQL}
    `);

    res.json({ data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});


// ========================
// 3. Products API for Dropdown
// ========================
router.get('/products', async (req, res) => {
  try {
    const [rows] = await db.query('SELECT Item_key, Description FROM Product_Dim ORDER BY Item_key');
    res.json({ products: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trong routes/olap.js (hoặc olap.js tùy project của bạn)


// Lấy danh sách city
router.get('/cities', async (req, res) => {
  try {
    const [rows] = await db.query('SELECT City_key, City_name FROM City_Dim ORDER BY City_name');
    res.json({ cities: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Lấy danh sách loại khách hàng (Type_name)
router.get('/customer-types', async (req, res) => {
  try {
    const [rows] = await db.query('SELECT DISTINCT Type_name FROM Customer_Dim ORDER BY Type_name');
    const types = rows.map(r => r.Type_name);
    res.json({ types });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
// Lấy danh sách năm
router.get('/years', async (req, res) => {
  try {
    const [rows] = await db.query('SELECT DISTINCT Year FROM Time_Dim ORDER BY Year');
    const years = rows.map(r => r.Year);
    res.json({ years });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// API OLAP mở rộng: Tổng hợp đồng thời theo sản phẩm và khách hàng
router.post('/sales-cube-product-customer', async (req, res) => {
  try {
    const { itemKeys, customerKeys, cityKeys, years, quarters, months } = req.body;

    let wheres = [];
    if (itemKeys?.length)
      wheres.push(`sf.Item_key IN (${itemKeys.map(Number).join(',')})`);
    if (customerKeys?.length)
      wheres.push(`sf.Customer_key IN (${customerKeys.map(Number).join(',')})`);
    if (cityKeys?.length)
      wheres.push(`sf.City_key IN (${cityKeys.map(Number).join(',')})`);
    if (years?.length)
      wheres.push(`t.Year IN (${years.map(Number).join(',')})`);
    if (quarters?.length)
      wheres.push(`t.Quarter IN (${quarters.map(Number).join(',')})`);
    if (months?.length)
      wheres.push(`t.Month IN (${months.map(Number).join(',')})`);

    const whereSQL = wheres.length ? `WHERE ${wheres.join(' AND ')}` : '';

    // Truy vấn OLAP: group by product + customer (+các chiều khác)
    const [rows] = await db.query(`
      SELECT
        p.Item_key,
        p.Description AS Product,
        c.Customer_key,
        c.Customer_name AS Customer,
        t.Year,
        t.Quarter,
        t.Month,
        SUM(sf.Units_sold) AS UNIT,
        SUM(sf.Dollars_sold) AS DOLLAR
      FROM Sales_Fact sf
      JOIN Product_Dim p ON sf.Item_key = p.Item_key
      JOIN Customer_Dim c ON sf.Customer_key = c.Customer_key
      JOIN Time_Dim t ON sf.Time_key = t.Time_key
      ${whereSQL}
      GROUP BY p.Item_key, c.Customer_key, t.Year, t.Quarter, t.Month
      ORDER BY p.Item_key, c.Customer_key, t.Year, t.Quarter, t.Month
    `);

    res.json({ data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
router.get('/customers', async (req, res) => {
  try {
    const [rows] = await db.query('SELECT Customer_key, Customer_name FROM Customer_Dim ORDER BY Customer_name');
    res.json({ customers: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
