import { LayoutDashboard, Truck, Settings } from "lucide-react";

function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <h2>StreamForge</h2>
        <span>Telemetry Platform</span>
      </div>

      <nav className="sidebar-nav">
        <button className="nav-item active">
          <LayoutDashboard size={20} />
          <span>Dashboard</span>
        </button>

        <button className="nav-item">
          <Truck size={20} />
          <span>Trucks</span>
        </button>

        <button className="nav-item">
          <Settings size={20} />
          <span>Settings</span>
        </button>
      </nav>
    </aside>
  );
}

export default Sidebar;