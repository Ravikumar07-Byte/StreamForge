import Sidebar from "./components/layout/Sidebar";
import Dashboard from "./components/dashboard/Dashboard";
import "./App.css";

function App() {
  return (
    <div className="app">
      <Sidebar />

      <div className="main-content">
        <Dashboard />
      </div>
    </div>
  );
}

export default App;