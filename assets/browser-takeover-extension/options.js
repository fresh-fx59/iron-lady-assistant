const DEFAULT_PORT = 18792;

async function load() {
  const stored = await chrome.storage.local.get(["relayPort", "relayToken"]);
  document.getElementById("port").value = String(stored.relayPort || DEFAULT_PORT);
  document.getElementById("token").value = String(stored.relayToken || "");
}

async function save() {
  const port = Number.parseInt(document.getElementById("port").value || "", 10) || DEFAULT_PORT;
  const token = String(document.getElementById("token").value || "").trim();
  await chrome.storage.local.set({ relayPort: port, relayToken: token });
  document.getElementById("status").textContent = `Saved relay settings for http://127.0.0.1:${port}`;
}

document.getElementById("save").addEventListener("click", () => void save());
void load();
