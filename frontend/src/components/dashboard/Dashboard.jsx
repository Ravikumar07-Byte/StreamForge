import { useCallback, useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_URL = "http://localhost:8000/api/telemetry";
const REFRESH_INTERVAL = 5000;

function Dashboard() {
  const [telemetry, setTelemetry] = useState([]);
  const [kafkaStatus, setKafkaStatus] = useState("Checking...");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const fetchDashboardData = useCallback(async () => {
    try {
      setError("");

      const response = await fetch(API_URL);

      if (!response.ok) {
        throw new Error(`Server returned ${response.status}`);
      }

      const data = await response.json();

      setTelemetry(
        Array.isArray(data.telemetry) ? data.telemetry : []
      );

      setKafkaStatus(data.kafka_status || "Unknown");
    } catch (err) {
      console.error("Dashboard API error:", err);

      setError("Unable to connect to StreamForge backend.");
      setKafkaStatus("Offline");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDashboardData();

    const interval = setInterval(
      fetchDashboardData,
      REFRESH_INTERVAL
    );

    return () => clearInterval(interval);
  }, [fetchDashboardData]);

  // Total unique trucks
  const totalTrucks = useMemo(() => {
    return new Set(
      telemetry.map((item) => item.truck)
    ).size;
  }, [telemetry]);

  // Total telemetry events
  const telemetryEvents = telemetry.length;

  // Temperature alerts
  const alerts = useMemo(() => {
    return telemetry.filter(
      (item) => Number(item.temperature) >= 35
    ).length;
  }, [telemetry]);

  // Chart data
  const chartData = useMemo(() => {
    return telemetry.slice(-20).map((item) => ({
      time: item.timestamp
        ? new Date(item.timestamp).toLocaleTimeString()
        : "--",
      temperature: Number(item.temperature),
      truck: item.truck,
    }));
  }, [telemetry]);

  // Latest 10 events
  const recentTelemetry = useMemo(() => {
    return [...telemetry]
      .reverse()
      .slice(0, 10);
  }, [telemetry]);

  const getStatus = (temperature) => {
    const value = Number(temperature);

    if (value >= 35) {
      return "Warning";
    }

    return "Normal";
  };

  if (loading) {
    return (
      <main className="dashboard">
        <section className="welcome-card">
          <h2>Welcome to StreamForge</h2>
          <p>
            Connecting to the telemetry service...
          </p>
        </section>

        <div className="dashboard-loading">
          Loading dashboard data...
        </div>
      </main>
    );
  }

  return (
    <main className="dashboard">

      {/* Welcome */}
      <section className="welcome-card">
        <h2>Welcome to StreamForge</h2>

        <p>
          Monitor truck telemetry and streaming activity
          from one place.
        </p>

        {error && (
          <div className="dashboard-error">
            {error}
          </div>
        )}
      </section>

      {/* KPI Cards */}
      <section className="stats-grid">

        <div className="stat-card">
          <span className="stat-label">
            Total Trucks
          </span>

          <strong>
            {totalTrucks}
          </strong>

          <small>
            Active vehicles
          </small>
        </div>

        <div className="stat-card">
          <span className="stat-label">
            Telemetry Events
          </span>

          <strong>
            {telemetryEvents}
          </strong>

          <small>
            Events received
          </small>
        </div>

        <div className="stat-card">
          <span className="stat-label">
            Kafka Status
          </span>

          <strong
            className={
              kafkaStatus.toLowerCase() === "online"
                ? "status-online"
                : "status-offline"
            }
          >
            {kafkaStatus}
          </strong>

          <small>
            Stream connection
          </small>
        </div>

        <div className="stat-card">
          <span className="stat-label">
            Alerts
          </span>

          <strong>
            {alerts}
          </strong>

          <small>
            Temperature warnings
          </small>
        </div>

      </section>

      {/* Temperature Chart */}
      <section className="dashboard-card chart-card">

        <div className="card-header">

          <div>
            <h3>
              Temperature Monitoring
            </h3>

            <p>
              Live truck temperature telemetry
            </p>
          </div>

          <span className="live-indicator">
            ● LIVE
          </span>

        </div>

        <div className="chart-container">

          {chartData.length === 0 ? (
            <div className="empty-state">
              No telemetry data available.
            </div>
          ) : (
            <ResponsiveContainer
              width="100%"
              height="100%"
            >
              <LineChart data={chartData}>

                <CartesianGrid
                  strokeDasharray="3 3"
                />

                <XAxis
                  dataKey="time"
                />

                <YAxis
                  domain={["auto", "auto"]}
                />

                <Tooltip
                  formatter={(value) => [
                    `${value} °C`,
                    "Temperature",
                  ]}
                  labelFormatter={(label) =>
                    `Time: ${label}`
                  }
                />

                <Line
                  type="monotone"
                  dataKey="temperature"
                  strokeWidth={3}
                  dot={{ r: 4 }}
                  activeDot={{ r: 6 }}
                />

              </LineChart>
            </ResponsiveContainer>
          )}

        </div>

      </section>

      {/* Recent Telemetry */}
      <section className="dashboard-card">

        <div className="card-header">

          <div>
            <h3>
              Recent Telemetry
            </h3>

            <p>
              Latest events received from trucks
            </p>
          </div>

          <button
            className="refresh-button"
            onClick={fetchDashboardData}
          >
            Refresh
          </button>

        </div>

        <div className="table-wrapper">

          {recentTelemetry.length === 0 ? (
            <div className="empty-state">
              No telemetry events received yet.
            </div>
          ) : (
            <table className="telemetry-table">

              <thead>
                <tr>
                  <th>Truck</th>
                  <th>Temperature</th>
                  <th>Time</th>
                  <th>Status</th>
                </tr>
              </thead>

              <tbody>

                {recentTelemetry.map(
                  (item, index) => {
                    const status = getStatus(
                      item.temperature
                    );

                    return (
                      <tr
                        key={`${item.truck}-${item.timestamp}-${index}`}
                      >

                        <td>
                          {item.truck}
                        </td>

                        <td>
                          {Number(
                            item.temperature
                          ).toFixed(1)}{" "}
                          °C
                        </td>

                        <td>
                          {item.timestamp
                            ? new Date(
                                item.timestamp
                              ).toLocaleString()
                            : "--"}
                        </td>

                        <td>
                          <span
                            className={
                              status === "Warning"
                                ? "status-badge warning"
                                : "status-badge normal"
                            }
                          >
                            {status}
                          </span>
                        </td>

                      </tr>
                    );
                  }
                )}

              </tbody>

            </table>
          )}

        </div>

      </section>

    </main>
  );
}

export default Dashboard;