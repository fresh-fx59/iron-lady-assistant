let relayWs = null;
const tabs = new Map();

async function getSettings() {
  const stored = await chrome.storage.local.get(["relayPort", "relayToken"]);
  return {
    port: Number.parseInt(String(stored.relayPort || "18792"), 10) || 18792,
    token: String(stored.relayToken || "").trim(),
  };
}

async function ensureRelay() {
  if (relayWs && relayWs.readyState === WebSocket.OPEN) {
    return relayWs;
  }
  const settings = await getSettings();
  const url = `ws://127.0.0.1:${settings.port}/extension?token=${encodeURIComponent(settings.token)}`;
  relayWs = new WebSocket(url);
  relayWs.onmessage = (event) => {
    const data = JSON.parse(String(event.data || "{}"));
    if (data.type === "cdp_command") {
      const target = { tabId: Number(data.tabId) };
      chrome.debugger.sendCommand(target, String(data.method), data.params || {}, (result) => {
        const error = chrome.runtime.lastError?.message || null;
        relayWs.send(JSON.stringify({ type: "cdp_response", id: String(data.id), result, error }));
      });
    }
  };
  await new Promise((resolve, reject) => {
    relayWs.onopen = () => resolve();
    relayWs.onerror = () => reject(new Error("relay connection failed"));
  });
  return relayWs;
}

async function attachTab(tab) {
  const ws = await ensureRelay();
  await chrome.debugger.attach({ tabId: tab.id }, "1.3");
  tabs.set(tab.id, true);
  ws.send(JSON.stringify({ type: "attach", tabId: tab.id, title: tab.title || "", url: tab.url || "" }));
  chrome.action.setBadgeText({ tabId: tab.id, text: "ON" });
}

async function detachTab(tabId) {
  if (!tabs.get(tabId)) {
    return;
  }
  try {
    await chrome.debugger.detach({ tabId });
  } catch {
    // ignore
  }
  tabs.delete(tabId);
  if (relayWs && relayWs.readyState === WebSocket.OPEN) {
    relayWs.send(JSON.stringify({ type: "detach", tabId }));
  }
  chrome.action.setBadgeText({ tabId, text: "" });
}

function sendTabUpdate(tabId, tab) {
  if (!tabs.get(tabId) || !relayWs || relayWs.readyState !== WebSocket.OPEN) {
    return;
  }
  relayWs.send(
    JSON.stringify({
      type: "tab_update",
      tabId,
      title: tab.title || "",
      url: tab.url || "",
    }),
  );
}

chrome.action.onClicked.addListener(async (tab) => {
  if (!tab.id) {
    return;
  }
  if (tabs.get(tab.id)) {
    await detachTab(tab.id);
    return;
  }
  await attachTab(tab);
});

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (!source.tabId || !relayWs || relayWs.readyState !== WebSocket.OPEN) {
    return;
  }
  relayWs.send(
    JSON.stringify({
      type: "cdp_event",
      tabId: source.tabId,
      method,
      params,
      url: params?.frame?.url || "",
    }),
  );
});

chrome.tabs.onRemoved.addListener((tabId) => {
  void detachTab(tabId);
});

chrome.tabs.onUpdated.addListener((tabId, _changeInfo, tab) => {
  sendTabUpdate(tabId, tab);
});
