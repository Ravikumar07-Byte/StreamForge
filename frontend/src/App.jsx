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
        {activePage === "dashboard" && <Dashboard />}

        {activePage === "trucks" && <Trucks />}

        {activePage === "analytics" && <Analytics />}

        {activePage === "topology" && <Topology />}
      </div>
    </div>
  );
}

export default App;