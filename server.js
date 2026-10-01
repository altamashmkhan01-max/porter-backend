// Porter backend MVP: catalog, orders, picker + rider flow, live updates (SSE).
// Run: npm install && STAFF_TOKEN=change-me node server.js
const express = require("express");
const Database = require("better-sqlite3");
const app = express();
app.use(express.json());
app.use(express.static(__dirname));
const db = new Database(process.env.DB || "porter.db");
db.pragma("journal_mode = WAL");

db.exec(`
CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY, name TEXT, size TEXT, price_cents INTEGER, stock INTEGER);
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY, customer_name TEXT, phone TEXT, address TEXT,
  subtotal_cents INTEGER, fee_cents INTEGER, tax_cents INTEGER, total_cents INTEGER,
  status TEXT DEFAULT 'placed', rider TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS order_items(order_id INTEGER, product_id INTEGER, qty INTEGER, price_cents INTEGER);
`);
if (!db.prepare("SELECT COUNT(*) c FROM products").get().c) {
  const ins = db.prepare("INSERT INTO products(name,size,price_cents,stock) VALUES(?,?,?,?)");
  [["2% milk","2 L",549,12],["Large eggs","12 ct",499,20],["Sourdough","600 g",599,8],
   ["Bananas","1 lb",129,30],["Avocados","2 ct",399,3],["Butter","454 g",649,10]].forEach(r => ins.run(...r));
}

const HST = 0.13, BASE_FEE = 299, SMALL_FEE = 200, FREE_OVER = 3500;
const FLOW = { placed: "picking", picking: "ready", ready: "out_for_delivery", out_for_delivery: "delivered" };

// Live updates
const clients = new Set();
const emit = (type, data) => clients.forEach(r => r.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`));
app.get("/api/events", (req, res) => {
  res.set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
  res.write(": ok\n\n"); clients.add(res); req.on("close", () => clients.delete(res));
});

// Staff auth (replace with real accounts before launch)
const staff = (req, res, next) =>
  req.get("x-staff-token") === (process.env.STAFF_TOKEN || "dev-token") ? next() : res.status(401).json({ error: "Staff token required" });

const getOrder = id => {
  const o = db.prepare("SELECT * FROM orders WHERE id=?").get(id);
  if (o) o.items = db.prepare("SELECT p.name, i.qty, i.price_cents FROM order_items i JOIN products p ON p.id=i.product_id WHERE order_id=?").all(id);
  return o;
};

app.get("/api/products", (_, res) => res.json(db.prepare("SELECT * FROM products WHERE stock>0").all()));

// Price + stock are always computed on the server, never trusted from the client.
app.post("/api/orders", (req, res) => {
  const { items, customer_name, phone, address } = req.body || {};
  if (!Array.isArray(items) || !items.length || !address) return res.status(400).json({ error: "items and address required" });
  try {
    const id = db.transaction(() => {
      let subtotal = 0; const lines = [];
      for (const { product_id, qty } of items) {
        const p = db.prepare("SELECT * FROM products WHERE id=?").get(product_id);
        if (!p || !Number.isInteger(qty) || qty < 1) throw new Error("Invalid item");
        if (p.stock < qty) throw new Error(`Only ${p.stock} of ${p.name} left`);
        db.prepare("UPDATE products SET stock=stock-? WHERE id=?").run(qty, p.id);
        subtotal += p.price_cents * qty; lines.push([p.id, qty, p.price_cents]);
      }
      const fee = BASE_FEE + (subtotal < FREE_OVER ? SMALL_FEE : 0);
      const tax = Math.round((subtotal + fee) * HST);
      const r = db.prepare("INSERT INTO orders(customer_name,phone,address,subtotal_cents,fee_cents,tax_cents,total_cents) VALUES(?,?,?,?,?,?,?)")
        .run(customer_name || "", phone || "", address, subtotal, fee, tax, subtotal + fee + tax);
      lines.forEach(l => db.prepare("INSERT INTO order_items VALUES(?,?,?,?)").run(r.lastInsertRowid, ...l));
      return r.lastInsertRowid;
    })();
    const order = getOrder(id); emit("order", order); res.status(201).json(order);
  } catch (e) { res.status(409).json({ error: e.message }); }
});

app.get("/api/orders/:id", (req, res) => getOrder(req.params.id) ? res.json(getOrder(req.params.id)) : res.sendStatus(404));

// Picker and rider endpoints
app.get("/api/staff/orders", staff, (_, res) =>
  res.json(db.prepare("SELECT id FROM orders WHERE status!='delivered' ORDER BY id").all().map(o => getOrder(o.id))));

app.post("/api/staff/orders/:id/advance", staff, (req, res) => {
  const o = getOrder(req.params.id); if (!o) return res.sendStatus(404);
  const next = FLOW[o.status]; if (!next) return res.status(409).json({ error: "Already delivered" });
  if (next === "out_for_delivery" && !req.body.rider) return res.status(400).json({ error: "rider required" });
  db.prepare("UPDATE orders SET status=?, rider=COALESCE(?,rider) WHERE id=?").run(next, req.body.rider || null, o.id);
  const updated = getOrder(o.id); emit("order", updated); res.json(updated);
});

app.post("/api/staff/products/:id/stock", staff, (req, res) => {
  db.prepare("UPDATE products SET stock=? WHERE id=?").run(req.body.stock, req.params.id);
  res.json({ ok: true });
});

app.listen(process.env.PORT || 3000, () => console.log("Porter API on :" + (process.env.PORT || 3000)));
