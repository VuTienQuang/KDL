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
router.get('/sales-cube', async (req, res) => {
  try {
    const itemKey = req.query.item_key;
    if (!itemKey) return res.status(400).json({ error: 'Missing item_key' });

    // Lấy các loại khách hàng
    const [types] = await db.query(`SELECT DISTINCT Type_name FROM Customer_Dim ORDER BY Type_name`);
    const typeList = types.map(t => t.Type_name);

    // Lấy data có Year, Quarter, Month
    const [rows] = await db.query(`
      SELECT
        c.State AS region,
        t.Year,
        t.Quarter,
        t.Month,
        cd.Type_name,
        SUM(sf.Units_sold) AS unit,
        SUM(sf.Dollars_sold) AS dollar
      FROM Sales_Fact sf
      JOIN City_Dim c     ON sf.City_key = c.City_key
      JOIN Time_Dim t     ON sf.Time_key = t.Time_key
      JOIN Customer_Dim cd ON sf.Customer_key = cd.Customer_key
      WHERE sf.Item_key = ?
      GROUP BY c.State, t.Year, t.Quarter, t.Month, cd.Type_name
      ORDER BY c.State, t.Year, t.Quarter, t.Month, cd.Type_name
    `, [itemKey]);

    // Build region > year > quarter > month, key luôn unique ở từng cấp
    const regionMap = {};
    rows.forEach(row => {
      // REGION
      if (!regionMap[row.region]) regionMap[row.region] = { key: `region-${row.region}`, city: row.region, children: {}, totals: {} };
      const region = regionMap[row.region];

      // YEAR
      if (!region.children[row.Year]) region.children[row.Year] = { key: `region-${row.region}-year-${row.Year}`, year: row.Year, children: {}, totals: {} };
      const year = region.children[row.Year];

      // QUARTER
      if (!year.children[row.Quarter]) year.children[row.Quarter] = { key: `region-${row.region}-year-${row.Year}-quarter-${row.Quarter}`, quarter: row.Quarter, children: {}, totals: {} };
      const quarter = year.children[row.Quarter];

      // MONTH
      if (!quarter.children[row.Month]) quarter.children[row.Month] = { key: `region-${row.region}-year-${row.Year}-quarter-${row.Quarter}-month-${row.Month}`, month: row.Month, customers: {} };
      const month = quarter.children[row.Month];

      // Đổ dữ liệu cho từng loại khách hàng
      month.customers[row.Type_name] = {
        unit: Number(row.unit),
        dollar: round2(row.dollar)
      };

      // Cộng dồn totals lên từng cấp
      [region, year, quarter].forEach(lv => {
        lv.totals[row.Type_name] = lv.totals[row.Type_name] || { unit: 0, dollar: 0 };
        lv.totals[row.Type_name].unit += Number(row.unit);
        lv.totals[row.Type_name].dollar += Number(row.dollar);
      });
    });

    // Build tree recursive, children luôn là mảng, key luôn unique
    function buildTree(level, lvType = 'region') {
  if (level.customers) {
    // Leaf: Month node
    let rowUnit = 0, rowDollar = 0;
    typeList.forEach(type => {
      rowUnit += level.customers[type]?.unit || 0;
      rowDollar += round2(level.customers[type]?.dollar || 0);
    });
    return {
      key: level.key,
      city: '',   // chỉ leaf mới có month, các cột khác rỗng
      year: '',
      quarter: '',
      month: level.month,
      ...typeList.reduce((obj, type) => ({
        ...obj,
        [type + '_UNIT']: level.customers[type]?.unit || 0,
        [type + '_DOLLAR']: round2(level.customers[type]?.dollar || 0)
      }), {}),
      total_unit: rowUnit,
      total_dollar: round2(rowDollar)
    };
  }

  // Build children
  const children = Object.values(level.children).map(child => {
    // Cấp dưới của region là year
    if ('year' in child) return buildTree(child, 'year');
    if ('quarter' in child) return buildTree(child, 'quarter');
    if ('month' in child) return buildTree(child, 'month');
    // fallback
    return buildTree(child, '');
  });

  // Set cột chỉ cho đúng cấp
  let row = {
    key: level.key,
    city: lvType === 'region' ? level.city : '',
    year: lvType === 'year' ? level.year : '',
    quarter: lvType === 'quarter' ? level.quarter : '',
    month: '', // không điền ở node cha
    ...typeList.reduce((obj, type) => ({
      ...obj,
      [type + '_UNIT']: level.totals[type]?.unit || 0,
      [type + '_DOLLAR']: round2(level.totals[type]?.dollar || 0)
    }), {}),
    total_unit: children.reduce((s, c) => s + (c.total_unit || 0), 0),
    total_dollar: round2(children.reduce((s, c) => s + (c.total_dollar || 0), 0)),
    children: children.length ? children : undefined
  };

  return row;
}


   const data = Object.values(regionMap).map(region => buildTree(region, 'region'));


    // Columns
    const columns = [
      { title: 'CITY', dataIndex: 'city', key: 'city', width: 140 },
      { title: 'YEAR', dataIndex: 'year', key: 'year', width: 80 },
      { title: 'QUARTER', dataIndex: 'quarter', key: 'quarter', width: 80 },
      { title: 'MONTH', dataIndex: 'month', key: 'month', width: 80 },
      ...typeList.flatMap(type => [
        { title: type, children: [
            { title: 'UNIT', dataIndex: type + '_UNIT', key: type + '_UNIT', align: 'right' },
            { title: 'DOLLAR', dataIndex: type + '_DOLLAR', key: type + '_DOLLAR', align: 'right' }
        ]}
      ]),
      { title: 'Total', children: [
          { title: 'UNIT', dataIndex: 'total_unit', key: 'total_unit', align: 'right' },
          { title: 'DOLLAR', dataIndex: 'total_dollar', key: 'total_dollar', align: 'right' }
      ]}
    ];

    res.json({ columns, data });
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
router.post('/sales-cube-dice', async (req, res) => {
  try {
    const { itemKeys, cityKeys, years, quarters, months, customerTypes } = req.body;

    // Tạo điều kiện động cho WHERE
    let wheres = [];
    if (itemKeys?.length)
      wheres.push(`sf.Item_key IN (${itemKeys.map(Number).join(',')})`);
    if (cityKeys?.length)
      wheres.push(`sf.City_key IN (${cityKeys.map(Number).join(',')})`);
    if (years?.length)
      wheres.push(`t.Year IN (${years.map(Number).join(',')})`);
    if (quarters?.length)
      wheres.push(`t.Quarter IN (${quarters.map(Number).join(',')})`);
    if (months?.length)
      wheres.push(`t.Month IN (${months.map(Number).join(',')})`);
    if (customerTypes?.length)
      wheres.push(`cd.Type_name IN (${customerTypes.map(v => `'${v}'`).join(',')})`);

    const whereSQL = wheres.length ? `WHERE ${wheres.join(' AND ')}` : '';

    // Lấy các loại khách hàng động
    const [types] = await db.query(`SELECT DISTINCT Type_name FROM Customer_Dim ORDER BY Type_name`);
    const typeList = types.map(t => t.Type_name);

    // Truy vấn tổng hợp OLAP (có thể nhóm theo các chiều cần thiết)
    const [rows] = await db.query(`
      SELECT
        c.State AS region,
        t.Year,
        t.Quarter,
        t.Month,
        cd.Type_name,
        SUM(sf.Units_sold) AS unit,
        SUM(sf.Dollars_sold) AS dollar
      FROM Sales_Fact sf
      JOIN City_Dim c     ON sf.City_key = c.City_key
      JOIN Time_Dim t     ON sf.Time_key = t.Time_key
      JOIN Customer_Dim cd ON sf.Customer_key = cd.Customer_key
      ${whereSQL}
      GROUP BY c.State, t.Year, t.Quarter, t.Month, cd.Type_name
      ORDER BY c.State, t.Year, t.Quarter, t.Month, cd.Type_name
    `);

    // (Có thể build tree hoặc trả data thẳng theo nhu cầu frontend)
    res.json({ data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

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
