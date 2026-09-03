import { useCallback, useEffect, useMemo, useState } from "react";

import {
  Activity,
  AlertTriangle,
  CarFront,
  CheckCircle2,
  Clock3,
  RefreshCw,
  Thermometer,
  Wifi,
  WifiOff,
} from "lucide-react";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import api from "../../services/api";

import "./Dashboard.css";

const REFRESH_INTERVAL = 5000;
const WARNING_TEMPERATURE = 35;

const EMPTY_METRICS = {
  events_received: 0,
  events_processed: 0,
  events_invalid: 0,
  events_late: 0,
  active_trucks: 0,
};

function Dashboard() {
  const [telemetry, setTelemetry] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [metrics, setMetrics] = useState(EMPTY_METRICS);

  const [kafkaStatus, setKafkaStatus] = useState("Checking");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);

  // ---------------------------------------------------------
  // FETCH BACKEND DATA
  // ---------------------------------------------------------

  const fetchDashboardData = useCallback(async (manual = false) => {
    try {
      if (manual) {
        setRefreshing(true);
      }

      const [telemetryResponse, metricsResponse] =
        await Promise.all([
          api.get("/api/telemetry"),
          api.get("/api/metrics"),
        ]);

      const telemetryData = telemetryResponse.data;
      const metricsData = metricsResponse.data;

      setTelemetry(
        Array.isArray(telemetryData.telemetry)
          ? telemetryData.telemetry
          : []
      );

      setAlerts(
        Array.isArray(telemetryData.alerts)
          ? telemetryData.alerts
          : []
      );

      setKafkaStatus(
        telemetryData.kafka_status || "Unknown"
      );

      setMetrics({
        events_received: Number(
          metricsData.events_received || 0
        ),
        events_processed: Number(
          metricsData.events_processed || 0
        ),
        events_invalid: Number(
          metricsData.events_invalid || 0
        ),
        events_late: Number(
          metricsData.events_late || 0
        ),
        active_trucks: Number(
          metricsData.active_trucks || 0
        ),
      });

      setError("");
      setLastUpdated(new Date());
    } catch (err) {
      console.error("Dashboard API error:", err);

      setKafkaStatus("Offline");

      setError(
        "Unable to connect to the StreamForge backend."
      );
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // ---------------------------------------------------------
  // AUTO REFRESH
  // ---------------------------------------------------------

  useEffect(() => {
    fetchDashboardData();

    const interval = setInterval(() => {
      fetchDashboardData();
    }, REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [fetchDashboardData]);

  // ---------------------------------------------------------
  // CALCULATIONS
  // ---------------------------------------------------------

  const averageTemperature = useMemo(() => {
    const values = telemetry
      .map((event) => Number(event.temperature))
      .filter((value) => Number.isFinite(value));

    if (!values.length) {
      return "--";
    }

    return (
      values.reduce((sum, value) => sum + value, 0) /
      values.length
    ).toFixed(1);
  }, [telemetry]);

  const chartData = useMemo(() => {
    return telemetry.slice(-20).map((event) => {
      const date = new Date(event.timestamp);

      return {
        time: Number.isNaN(date.getTime())
          ? "--"
          : date.toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
            }),
        temperature: Number(event.temperature),
        truck: event.truck || "--",
      };
    });
  }, [telemetry]);

  const recentTelemetry = useMemo(() => {
    return [...telemetry].reverse().slice(0, 8);
  }, [telemetry]);

  const isOnline =
    kafkaStatus.toLowerCase() === "online";

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return "--";

    const date = new Date(timestamp);

    if (Number.isNaN(date.getTime())) {
      return timestamp;
    }

    return date.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const getTemperatureClass = (temperature) => {
    return Number(temperature) >= WARNING_TEMPERATURE
      ? "temperature-warning"
      : "temperature-normal";
  };

  // ---------------------------------------------------------
  // LOADING
  // ---------------------------------------------------------

  if (loading) {
    return (
      <main className="dashboard-page">
        <div className="dashboard-loading">
          <RefreshCw className="spin" size={30} />

          <h2>Loading StreamForge</h2>

          <p>
            Connecting to the telemetry processing
            service...
          </p>
        </div>
      </main>
    );
  }

  // ---------------------------------------------------------
  // DASHBOARD
  // ---------------------------------------------------------

  return (
    <main className="dashboard-page">

      {/* HEADER */}

      <section className="dashboard-topbar">
        <div>
          <div className="dashboard-eyebrow">
            <Activity size={15} />
            REAL-TIME EVENT PROCESSOR
          </div>

          <h1>Overview</h1>

          <p>
            Monitor your streaming platform in real-time
          </p>
        </div>

        <div className="topbar-actions">

          <div
            className={`connection-pill ${
              isOnline ? "online" : "offline"
            }`}
          >
            {isOnline ? (
              <Wifi size={15} />
            ) : (
              <WifiOff size={15} />
            )}

            <span>
              Kafka {kafkaStatus}
            </span>
          </div>

          <div className="last-refresh">
            <Clock3 size={15} />

            {lastUpdated
              ? lastUpdated.toLocaleTimeString()
              : "--"}
          </div>

          <button
            className="dashboard-refresh"
            onClick={() => fetchDashboardData(true)}
            disabled={refreshing}
          >
            <RefreshCw
              size={16}
              className={refreshing ? "spin" : ""}
            />

            {refreshing
              ? "Refreshing..."
              : "Refresh"}
          </button>

        </div>
      </section>

      {/* ERROR */}

      {error && (
        <div className="dashboard-error">
          <AlertTriangle size={18} />
          {error}
        </div>
      )}

      {/* KPI CARDS */}

      <section className="kpi-grid">

        {/* EVENTS PROCESSED */}

        <div className="kpi-card blue-card">

          <div className="kpi-icon">
            <Activity size={22} />
          </div>

          <div className="kpi-content">

            <span>Events Processed</span>

            <strong>
              {metrics.events_processed.toLocaleString()}
            </strong>

            <small>
              Successfully processed
            </small>

          </div>

        </div>

        {/* ACTIVE TRUCKS */}

        <div className="kpi-card green-card">

          <div className="kpi-icon">
            <CarFront size={22} />
          </div>

          <div className="kpi-content">

            <span>Active Trucks</span>

            <strong>
              {metrics.active_trucks}
            </strong>

            <small>
              Vehicles reporting telemetry
            </small>

          </div>

        </div>

        {/* AVERAGE TEMPERATURE */}

        <div className="kpi-card orange-card">

          <div className="kpi-icon">
            <Thermometer size={22} />
          </div>

          <div className="kpi-content">

            <span>Average Temperature</span>

            <strong>
              {averageTemperature}
              <em>°C</em>
            </strong>

            <small>
              Based on recent telemetry
            </small>

          </div>

        </div>

        {/* ACTIVE ALERTS */}

        <div className="kpi-card red-card">

          <div className="kpi-icon">
            <AlertTriangle size={22} />
          </div>

          <div className="kpi-content">

            <span>Active Alerts</span>

            <strong>
              {alerts.length}
            </strong>

            <small>
              Temperature warnings
            </small>

          </div>

        </div>

      </section>

      {/* MAIN GRID */}

      <section className="content-grid">

        {/* TEMPERATURE CHART */}

        <div className="dashboard-panel chart-panel">

          <div className="panel-title">

            <div>

              <div className="title-row">

                <h2>
                  Temperature Monitoring
                </h2>

                <span className="live-badge">
                  <span />
                  LIVE
                </span>

              </div>

              <p>
                Latest truck temperature telemetry
              </p>

            </div>

            <span className="panel-meta">
              Last 20 events
            </span>

          </div>

          <div className="chart-area">

            {chartData.length === 0 ? (

              <div className="empty-state">

                <Thermometer size={34} />

                <h3>
                  No telemetry data
                </h3>

                <p>
                  Start the telemetry producer to
                  see live data.
                </p>

              </div>

            ) : (

              <ResponsiveContainer
                width="100%"
                height="100%"
              >

                <LineChart
                  data={chartData}
                  margin={{
                    top: 10,
                    right: 20,
                    left: 0,
                    bottom: 5,
                  }}
                >

                  <CartesianGrid
                    strokeDasharray="3 3"
                    vertical={false}
                    stroke="rgba(148,163,184,0.12)"
                  />

                  <XAxis
                    dataKey="time"
                    tick={{
                      fill: "#7f8da3",
                      fontSize: 11,
                    }}
                    tickLine={false}
                    axisLine={false}
                  />

                  <YAxis
                    tick={{
                      fill: "#7f8da3",
                      fontSize: 11,
                    }}
                    tickLine={false}
                    axisLine={false}
                    unit="°C"
                  />

                  <Tooltip
                    contentStyle={{
                      background: "#111827",
                      border:
                        "1px solid #263247",
                      borderRadius: "8px",
                      color: "#fff",
                    }}
                    formatter={(value) => [
                      `${value} °C`,
                      "Temperature",
                    ]}
                  />

                  <Line
                    type="monotone"
                    dataKey="temperature"
                    stroke="#3b82f6"
                    strokeWidth={3}
                    dot={{
                      r: 3,
                      fill: "#3b82f6",
                    }}
                    activeDot={{
                      r: 6,
                    }}
                  />

                </LineChart>

              </ResponsiveContainer>
            )}

          </div>

        </div>

        {/* SYSTEM HEALTH */}

        <div className="dashboard-panel health-panel">

          <div className="panel-title">

            <div>

              <h2>
                Pipeline Health
              </h2>

              <p>
                Current service status
              </p>

            </div>

            {isOnline ? (
              <CheckCircle2
                className="healthy-icon"
                size={22}
              />
            ) : (
              <WifiOff
                className="offline-icon"
                size={22}
              />
            )}

          </div>

          <div className="health-main">

            <div
              className={`health-status ${
                isOnline
                  ? "healthy"
                  : "unhealthy"
              }`}
            >
              <span />

              {isOnline
                ? "All systems operational"
                : "System unavailable"}
            </div>

            <div className="health-row">
              <span>Kafka</span>
              <strong>{kafkaStatus}</strong>
            </div>

            <div className="health-row">
              <span>FastAPI</span>

              <strong className="green-text">
                Online
              </strong>
            </div>

            <div className="health-row">
              <span>Consumer</span>

              <strong className="green-text">
                Running
              </strong>
            </div>

            <div className="health-row">
              <span>Active Trucks</span>

              <strong>
                {metrics.active_trucks}
              </strong>
            </div>

            <div className="health-row">
              <span>Invalid Events</span>

              <strong>
                {metrics.events_invalid}
              </strong>
            </div>

          </div>

        </div>

      </section>

      {/* RECENT EVENTS */}

      <section className="dashboard-panel">

        <div className="panel-title">

          <div>

            <h2>
              Recent Telemetry
            </h2>

            <p>
              Latest events received from the truck fleet
            </p>

          </div>

          <span className="event-count">
            {telemetry.length} events
          </span>

        </div>

        <div className="table-wrapper">

          {recentTelemetry.length === 0 ? (

            <div className="empty-state">

              <Activity size={30} />

              <h3>
                No telemetry events
              </h3>

              <p>
                Waiting for truck telemetry from Kafka.
              </p>

            </div>

          ) : (

            <table className="modern-table">

              <thead>

                <tr>
                  <th>Truck</th>
                  <th>Temperature</th>
                  <th>Timestamp</th>
                  <th>Status</th>
                </tr>

              </thead>

              <tbody>

                {recentTelemetry.map(
                  (event, index) => {

                    const warning =
                      Number(event.temperature) >=
                      WARNING_TEMPERATURE;

                    return (
                      <tr
                        key={`${event.truck}-${event.timestamp}-${index}`}
                      >

                        <td>

                          <div className="truck-info">

                            <div className="truck-icon">
                              <CarFront size={16} />
                            </div>

                            <strong>
                              {event.truck}
                            </strong>

                          </div>

                        </td>

                        <td>

                          <strong
                            className={
                              getTemperatureClass(
                                event.temperature
                              )
                            }
                          >
                            {Number(
                              event.temperature
                            ).toFixed(1)}
                            °C
                          </strong>

                        </td>

                        <td className="timestamp">

                          {formatTimestamp(
                            event.timestamp
                          )}

                        </td>

                        <td>

                          <span
                            className={`status-badge ${
                              warning
                                ? "warning"
                                : "normal"
                            }`}
                          >

                            <span />

                            {warning
                              ? "Warning"
                              : "Normal"}

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

      {/* ACTIVE ALERTS */}

      <section className="dashboard-panel alerts-panel">

        <div className="panel-title">

          <div>

            <div className="title-row">

              <h2>
                Active Temperature Alerts
              </h2>

              {alerts.length > 0 && (
                <span className="alert-count">
                  {alerts.length}
                </span>
              )}

            </div>

            <p>
              Trucks currently above the 35°C threshold
            </p>

          </div>

          <AlertTriangle
            className={
              alerts.length
                ? "alert-icon-active"
                : "alert-icon"
            }
            size={22}
          />

        </div>

        {alerts.length === 0 ? (

          <div className="no-alerts">

            <CheckCircle2 size={30} />

            <div>

              <strong>
                No active temperature alerts
              </strong>

              <span>
                All trucks are currently within the
                temperature threshold.
              </span>

            </div>

          </div>

        ) : (

          <div className="alerts-list">

            {alerts.map((alert) => (

              <div
                className="alert-row"
                key={alert.truck_id}
              >

                <div className="alert-truck">

                  <div className="alert-truck-icon">
                    <Thermometer size={18} />
                  </div>

                  <div>

                    <strong>
                      {alert.truck_id}
                    </strong>

                    <span>
                      {alert.message}
                    </span>

                  </div>

                </div>

                <div className="alert-temperature">

                  <strong>
                    {Number(
                      alert.temperature
                    ).toFixed(1)}
                    °C
                  </strong>

                  <span>
                    Threshold{" "}
                    {Number(
                      alert.threshold
                    ).toFixed(1)}
                    °C
                  </span>

                </div>

                <span className="severity-badge">
                  {alert.severity || "warning"}
                </span>

                <span className="alert-time">

                  {formatTimestamp(
                    alert.updated_at ||
                    alert.timestamp
                  )}

                </span>

              </div>

            ))}

          </div>

        )}

      </section>

      <footer className="dashboard-footer">
        © 2026 StreamForge. Real-time Truck Telemetry Platform.
      </footer>

    </main>
  );
}

export default Dashboard;