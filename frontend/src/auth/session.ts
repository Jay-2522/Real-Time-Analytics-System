import { createContext, useContext } from "react";

export type User = { id: string; username: string; role: "viewer" | "analyst" | "administrator" };
export const SessionContext = createContext<{ user: User; logout: () => Promise<void> } | null>(null);
export function useSession() {
  const session = useContext(SessionContext);
  if (!session) throw new Error("Session provider required");
  return session;
}
