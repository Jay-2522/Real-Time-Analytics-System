import { SessionGate } from "./auth/SessionGate";
import { Navigate, Route, Routes } from "react-router-dom";

import { DashboardLayout } from "./layouts/DashboardLayout";
import { AlertsPage } from "./pages/AlertsPage";
import { AttackTimelinePage } from "./pages/AttackTimelinePage";
import { DetectionEngineeringPage } from "./pages/DetectionEngineeringPage";
import { EntitiesPage } from "./pages/EntitiesPage";
import { IncidentsPage } from "./pages/IncidentsPage";
import { LiveStreamPage } from "./pages/LiveStreamPage";
import { MitreAttackPage } from "./pages/MitreAttackPage";
import { MLPredictionsPage } from "./pages/MLPredictionsPage";
import { ObservabilityPage } from "./pages/ObservabilityPage";
import { OverviewPage } from "./pages/OverviewPage";
import { RiskAnalyticsPage } from "./pages/RiskAnalyticsPage";
import { SearchPage } from "./pages/SearchPage";
import { SystemHealthPage } from "./pages/SystemHealthPage";
import { ThreatHuntingPage } from "./pages/ThreatHuntingPage";
import { ThreatIntelligencePage } from "./pages/ThreatIntelligencePage";

export default function App() {
  return (
    <SessionGate><Routes>
      <Route element={<DashboardLayout />}>
        <Route index element={<OverviewPage />} />
        <Route path="live" element={<LiveStreamPage />} />
        <Route path="risk" element={<RiskAnalyticsPage />} />
        <Route path="predictions" element={<MLPredictionsPage />} />
        <Route path="search" element={<SearchPage />} />
        <Route path="health" element={<SystemHealthPage />} />
        <Route path="alerts" element={<AlertsPage />} />
        <Route path="incidents" element={<IncidentsPage />} />
        <Route path="observability" element={<ObservabilityPage />} />
        <Route path="threat-intelligence" element={<ThreatIntelligencePage />} />
        <Route path="detection-engineering" element={<DetectionEngineeringPage />} />
        <Route path="threat-hunting" element={<ThreatHuntingPage />} />
        <Route path="mitre-attack" element={<MitreAttackPage />} />
        <Route path="entities" element={<EntitiesPage />} />
        <Route path="attack-timeline" element={<AttackTimelinePage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes></SessionGate>
  );
}
