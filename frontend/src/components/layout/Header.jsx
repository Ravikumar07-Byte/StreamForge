function Header() {
  return (
    <header className="header">
      <div>
        <h1>Dashboard</h1>
        <p>Real-time truck telemetry monitoring</p>
      </div>

      <div className="backend-status">
        <span className="status-dot"></span>
        Backend Online
      </div>
    </header>
  );
}

export default Header;