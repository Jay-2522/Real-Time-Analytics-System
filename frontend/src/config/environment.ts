const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";
const eventSource = import.meta.env.VITE_EVENT_SOURCE ?? "synthetic";

function dashboardWebSocketUrl() {
  const configured = import.meta.env.VITE_DASHBOARD_WS_URL ?? import.meta.env.VITE_WS_BASE_URL;
  if (configured) {
    return configured.endsWith("/ws/dashboard") ? configured : `${configured.replace(/\/$/, "")}/ws/dashboard`;
  }

  const url = new URL(apiBaseUrl, window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/, "")}/ws/dashboard`;
  url.search = "";
  url.hash = "";
  return url.toString();
}

function alertsWebSocketUrl() {
  const configured = import.meta.env.VITE_ALERTS_WS_URL;
  if (configured) return configured;
  return dashboardWebSocketUrl().replace("/ws/dashboard", "/ws/alerts");
}

export const config = {
  apiBaseUrl,
  eventSource,
  eventSourceLabel: eventSource === "synthetic" ? "Synthetic generator" : eventSource,
  dashboardWebSocketUrl: dashboardWebSocketUrl(),
  alertsWebSocketUrl: alertsWebSocketUrl(),
};
