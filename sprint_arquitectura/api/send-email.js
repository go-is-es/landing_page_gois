// /api/send-email.js

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { nombre, telefono, email } = req.body;

  if (!email) {
    return res.status(400).json({ error: "Email requerido" });
  }

  // 👇 aquí de momento solo probamos
  console.log("Nuevo lead:", { nombre, telefono, email });

  return res.status(200).json({ ok: true });
}