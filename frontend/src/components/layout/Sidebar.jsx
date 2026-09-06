import {
  LayoutDashboard,
  Truck,
  BarChart3,
  GitBranch,
  Settings,
} from "lucide-react";

function Sidebar({ activePage, setActivePage }) {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <h2>StreamForge</h2>
        <span>Telemetry Platform</span>
      </div>

      <nav className="sidebar-nav">
        <button
          className={`nav-item ${
            activePage === "dashboard" ? "active" : ""
          }`}
          onClick={() => setActivePage("dashboard")}
        >
          <LayoutDashboard size={20} />
          <span>Dashboard</span>
        </button>

        <button
          className={`nav-item ${
            activePage === "trucks" ? "active" : ""
          }`}
          onClick={() => setActivePage("trucks")}
        >
          <Truck size={20} />
          <span>Trucks</span>
        </button>

        <button
          className={`nav-item ${
            activePage === "analytics" ? "active" : ""
          }`}
          onClick={() => setActivePage("analytics")}
        >
          <BarChart3 size={20} />
          <span>Analytics</span>
        </button>

        <button
          className={`nav-item ${
            activePage === "topology" ? "active" : ""
          }`}
          onClick={() => setActivePage("topology")}
        >
          <GitBranch size={20} />
          <span>Topology</span>
        </button>

        <button
          className={`nav-item ${
            activePage === "settings" ? "active" : ""
          }`}
          onClick={() => setActivePage("settings")}
        >
          <Settings size={20} />
          <span>Settings</span>
        </button>
      </nav>
    </aside>
  );
}

export default Sidebar;