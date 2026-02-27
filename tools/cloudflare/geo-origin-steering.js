addEventListener("fetch", (event) => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  const country = request.cf && request.cf.country ? request.cf.country : "XX";
  const isRu = country === "RU";

  const primary = "cf-origin-main.aiengineerhelper.com";
  const ru = "cf-origin-ru.aiengineerhelper.com";

  const first = isRu ? ru : primary;
  const second = isRu ? primary : ru;

  const attempt = async (resolveOverride, label) => {
    const resp = await fetch(request, { cf: { resolveOverride } });
    const headers = new Headers(resp.headers);
    headers.set("X-Origin-Selected", label);
    headers.set("X-CF-Country", country);
    return new Response(resp.body, {
      status: resp.status,
      statusText: resp.statusText,
      headers,
    });
  };

  try {
    const firstResp = await attempt(first, first === ru ? "ru" : "main");
    if (firstResp.status < 500) {
      return firstResp;
    }
  } catch (e) {
    // Try backup origin below.
  }

  try {
    return await attempt(second, second === ru ? "ru-fallback" : "main-fallback");
  } catch (e) {
    return new Response("Upstream unavailable", { status: 503 });
  }
}
