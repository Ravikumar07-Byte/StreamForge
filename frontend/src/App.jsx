import { useState } from "react";

import Sidebar from "./components/layout/Sidebar";
import Dashboard from "./components/dashboard/Dashboard";
import Trucks from "./components/trucks/trucks";
import Analytics from "./components/Analytics/Analytics";
import Topology from "./components/Topology/Topology";

import "./App.css";

function App() {
  const [activePage, setActivePage] = useState("dashboard");

  return (
    <div className="app">
      <Sidebar
        activePage={activePage}
        setActivePage={setActivePage}
      />

      <div className="main-content">
        {activePage === "dashboard" && (
          <Dashboard />
        )}

        {activePage === "trucks" && (
          <Trucks />
        )}

        {activePage === "analytics" && (
          <Analytics />
        )}

        {activePage === "topology" && (
          <Topology />
        )}

        {activePage === "settings" && (
          <main className="dashboard-page">
            <section className="dashboard-topbar">
              <div>
                <div className="dashboard-eyebrow">
                  SETTINGS
                </div>

                <h1>Settings</h1>

                <p>
                  Configure your StreamForge platform
                </p>
              </div>
            </section>

            <section className="dashboard-panel">
              <div className="panel-title">
                <div>
                  <h2>System Settings</h2>

                  <p>
                    StreamForge configuration options
                  </p>
                </div>
              </div>

              <div className="empty-state">
                <h3>Settings Ready</h3>

                <p>
                  Configuration options will be available here.
                </p>
              </div>
            </section>
          </main>
        )}
      </div>
    </div>
  );
}

export default App;