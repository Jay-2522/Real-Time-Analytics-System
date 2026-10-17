import axios from "axios";
import { config } from "../config/environment";

// Memory only. Session identifiers are exclusively in an HttpOnly cookie.
let csrfToken = "";
export function setCsrfToken(value: string) { csrfToken = value; }
export const httpClient = axios.create({
  baseURL: config.apiBaseUrl,
  timeout: 10_000,
  withCredentials: true,
  headers: { "Content-Type": "application/json", "X-Requested-With": "SOC" },
});
httpClient.interceptors.request.use((request) => {
  if (!["get", "head", "options"].includes(request.method ?? "get")) {
    request.headers.set("X-CSRF-Token", csrfToken);
  }
  return request;
});
httpClient.interceptors.response.use((response) => response, (error) => {
  if (axios.isAxiosError(error) && error.config?.url !== "/auth/login") {
    if (error.response?.status === 401) window.dispatchEvent(new Event("session-expired"));
    if (error.response?.status === 403) window.dispatchEvent(new Event("access-forbidden"));
  }
  return Promise.reject(error);
});
export function rethrowAuthError(error: unknown) {
  if (axios.isAxiosError(error) && [401, 403].includes(error.response?.status ?? 0)) throw error;
}
