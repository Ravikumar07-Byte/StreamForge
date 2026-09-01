import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  CarFront,
  CheckCircle2,
  Clock3,
  RefreshCw,
  Server,
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
  const [metrics, setMetrics] = useState(EMPTY_METRICS);

  const [kafkaStatus, setKafkaStatus] = useState("Checking");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);

  // ---------------------------------------------------------
  // Fetch dashboard data
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
        "Unable to connect to the StreamForge backend. Make sure the FastAPI server is running."
      );
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // ---------------------------------------------------------
  // Automatic refresh
  // ---------------------------------------------------------

  useEffect(() => {
    fetchDashboardData();

    const interval = setInterval(() => {
      fetchDashboardData();
    }, REFRESH_INTERVAL);

    return () => {
      clearInterval(interval);
    };
  }, [fetchDashboardData]);

  // ---------------------------------------------------------
  // Calculated dashboard values
  // ---------------------------------------------------------

  const warningCount = useMemo(() => {
    return telemetry.filter((event) => {
      return (
        Number(event.temperature) >=
        WARNING_TEMPERATURE
      );
    }).length;
  }, [telemetry]);

  const averageTemperature = useMemo(() => {
    const validTemperatures = telemetry
      .map((event) => Number(event.temperature))
      .filter((value) => Number.isFinite(value));

    if (validTemperatures.length === 0) {
      return "--";
    }

    const total = validTemperatures.reduce(
      (sum, value) => sum + value,
      0
    );

    return `${(
      total / validTemperatures.length
    ).toFixed(1)} °C`;
  }, [telemetry]);

  const chartData = useMemo(() => {
    return telemetry.slice(-20).map((event) => {
      const date = new Date(event.timestamp);

      return {
        time: Number.isNaN(date.getTime())
          ? event.timestamp || "--"
          : date.toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            }),
        temperature: Number(event.temperature),
        truck: event.truck || "--",
      };
    });
  }, [telemetry]);

  const recentTelemetry = useMemo(() => {
    return [...telemetry]
      .reverse()
      .slice(0, 10);
  }, [telemetry]);

  // ---------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------

  const formatTimestamp = (timestamp) => {
    if (!timestamp) {
      return "--";
    }

    const date = new Date(timestamp);

    if (Number.isNaN(date.getTime())) {
      return timestamp;
    }

    return date.toLocaleString();
  };

  const getTemperatureStatus = (temperature) => {
    const value = Number(temperature);

    if (value >= WARNING_TEMPERATURE) {
      return {
        label: "Warning",
        className: "status-badge warning",
      };
    }

    return {
      label: "Normal",
      className: "status-badge normal",
    };
  };

  const isOnline =
    kafkaStatus.toLowerCase() === "online";

  // ---------------------------------------------------------
  // Loading state
  // ---------------------------------------------------------

  if (loading) {
    return (
      <main className="dashboard">
        <section className="dashboard-loading">
          <div className="loading-spinner">
            <RefreshCw size={28} />
          </div>

          <h2>Loading StreamForge</h2>

          <p>
            Connecting to the telemetry processing
            service...
          </p>
        </section>
      </main>
    );
  }

  // ---------------------------------------------------------
  // Dashboard
  // ---------------------------------------------------------

  return (
    <main className="dashboard">

      {/* =====================================================
          HEADER
      ===================================================== */}

      <section className="dashboard-header">

        <div>
          <div className="eyebrow">
            <Activity size={16} />
            REAL-TIME MONITORING
          </div>

          <h1>Fleet Telemetry</h1>

          <p>
            Monitor truck temperature and streaming
            activity in real time.
          </p>
        </div>

        <div className="header-actions">

          <div
            className={`connection-status ${
              isOnline ? "online" : "offline"
            }`}
          >
            {isOnline ? (
              <Wifi size={16} />
            ) : (
              <WifiOff size={16} />
            )}

            <span>
              Kafka {kafkaStatus}
            </span>
          </div>

          <button
            type="button"
            className="refresh-button"
            onClick={() =>
              fetchDashboardData(true)
            }
            disabled={refreshing}
          >
            <RefreshCw
              size={16}
              className={
                refreshing ? "spin" : ""
              }
            />

            {refreshing
              ? "Refreshing..."
              : "Refresh"}
          </button>

        </div>

      </section>

      {/* =====================================================
          ERROR
      ===================================================== */}

      {error && (
        <section className="dashboard-alert error">

          <AlertTriangle size={20} />

          <div>
            <strong>
              Backend connection problem
            </strong>

            <p>{error}</p>
          </div>

        </section>
      )}

      {/* =====================================================
          KPI CARDS
      ===================================================== */}

      <section className="stats-grid">

        {/* Active Trucks */}

        <div className="stat-card">

          <div className="stat-card-top">

            <div className="stat-icon blue">
              <CarFront size={21} />
            </div>

            <span className="stat-trend">
              LIVE
            </span>

          </div>

          <span className="stat-label">
            Active Trucks
          </span>

          <strong className="stat-value">
            {metrics.active_trucks}
          </strong>

          <small>
            Vehicles reporting telemetry
          </small>

        </div>

        {/* Events Processed */}

        <div className="stat-card">

          <div className="stat-card-top">

            <div className="stat-icon green">
              <Activity size={21} />
            </div>

            <span className="stat-trend positive">
              STREAMING
            </span>

          </div>

          <span className="stat-label">
            Events Processed
          </span>

          <strong className="stat-value">
            {metrics.events_processed}
          </strong>

          <small>
            Successfully processed events
          </small>

        </div>

        {/* Average Temperature */}

        <div className="stat-card">

          <div className="stat-card-top">

            <div className="stat-icon orange">
              <Thermometer size={21} />
            </div>

            <span className="stat-trend">
              AVERAGE
            </span>

          </div>

          <span className="stat-label">
            Average Temperature
          </span>

          <strong className="stat-value">
            {averageTemperature}
          </strong>

          <small>
            Based on recent telemetry
          </small>

        </div>

        {/* Alerts */}

        <div className="stat-card">

          <div className="stat-card-top">

            <div className="stat-icon red">
              <AlertTriangle size={21} />
            </div>

            <span className="stat-trend danger">
              ATTENTION
            </span>

          </div>

          <span className="stat-label">
            Temperature Alerts
          </span>

          <strong className="stat-value">
            {warningCount}
          </strong>

          <small>
            Readings above 35 °C
          </small>

        </div>

      </section>

      {/* =====================================================
          SYSTEM OVERVIEW
      ===================================================== */}

      <section className="overview-grid">

        {/* Processing Statistics */}

        <div className="dashboard-card">

          <div className="card-header">

            <div>
              <h2>System Overview</h2>

              <p>
                Current StreamForge processing
                statistics
              </p>
            </div>

            <Server size={21} />

          </div>

          <div className="system-stats">

            <div className="system-stat">
              <span>Events Received</span>

              <strong>
                {metrics.events_received}
              </strong>
            </div>

            <div className="system-stat">
              <span>Events Processed</span>

              <strong>
                {metrics.events_processed}
              </strong>
            </div>

            <div className="system-stat">
              <span>Invalid Events</span>

              <strong>
                {metrics.events_invalid}
              </strong>
            </div>

            <div className="system-stat">
              <span>Late Events</span>

              <strong>
                {metrics.events_late}
              </strong>
            </div>

          </div>

        </div>

        {/* Pipeline Health */}

        <div className="dashboard-card health-card">

          <div className="card-header">

            <div>
              <h2>Pipeline Health</h2>

              <p>
                Kafka → Processing → Dashboard
              </p>
            </div>

            {isOnline ? (
              <CheckCircle2
                className="health-icon"
                size={24}
              />
            ) : (
              <WifiOff
                className="health-icon"
                size={24}
              />
            )}

          </div>

          <div className="health-content">

            <div
              className={`health-status ${
                isOnline
                  ? "healthy"
                  : "unhealthy"
              }`}
            >
              <span className="health-dot" />

              {isOnline
                ? "All systems operational"
                : "Backend connection unavailable"}
            </div>

            <div className="health-row">
              <span>Kafka Connection</span>

              <strong>
                {kafkaStatus}
              </strong>
            </div>

            <div className="health-row">
              <span>Active Trucks</span>

              <strong>
                {metrics.active_trucks}
              </strong>
            </div>

            <div className="health-row">
              <span>Last Refresh</span>

              <strong>
                {lastUpdated
                  ? lastUpdated.toLocaleTimeString()
                  : "--"}
              </strong>
            </div>

          </div>

        </div>

      </section>

      {/* =====================================================
          TEMPERATURE CHART
      ===================================================== */}

      <section className="dashboard-card chart-card">

        <div className="card-header">

          <div>

            <div className="title-with-status">

              <h2>
                Temperature Monitoring
              </h2>

              <span className="live-badge">
                <span className="live-dot" />
                LIVE
              </span>

            </div>

            <p>
              Recent truck temperature
              telemetry
            </p>

          </div>

          <div className="chart-info">
            <Clock3 size={16} />
            Last 20 events
          </div>

        </div>

        <div className="chart-container">

          {chartData.length === 0 ? (

            <div className="empty-state">

              <Thermometer size={34} />

              <h3>
                No telemetry data
              </h3>

              <p>
                Publish a telemetry event to
                Kafka to see the live chart.
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
                />

                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  axisLine={false}
                />

                <YAxis
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  axisLine={false}
                  domain={["auto", "auto"]}
                  unit="°C"
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
                  stroke="#2563eb"
                  strokeWidth={3}
                  dot={{
                    r: 4,
                  }}
                  activeDot={{
                    r: 7,
                  }}
                  animationDuration={500}
                />

              </LineChart>

            </ResponsiveContainer>

          )}

        </div>

      </section>

      {/* =====================================================
          RECENT TELEMETRY
      ===================================================== */}

      <section className="dashboard-card">

        <div className="card-header">

          <div>

            <h2>
              Recent Telemetry
            </h2>

            <p>
              Latest events received from the
              truck fleet
            </p>

          </div>

          <span className="event-count">
            {telemetry.length} events
          </span>

        </div>

        <div className="table-wrapper">

          {recentTelemetry.length === 0 ? (

            <div className="empty-state table-empty">

              <Activity size={32} />

              <h3>
                No telemetry events
              </h3>

              <p>
                Waiting for truck telemetry
                from Kafka.
              </p>

            </div>

          ) : (

            <table className="telemetry-table">

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

                    const status =
                      getTemperatureStatus(
                        event.temperature
                      );

                    return (
                      <tr
                        key={`${event.truck}-${event.timestamp}-${index}`}
                      >

                        <td>

                          <div className="truck-cell">

                            <div className="truck-avatar">
                              <CarFront
                                size={16}
                              />
                            </div>

                            <strong>
                              {event.truck ||
                                "--"}
                            </strong>

                          </div>

                        </td>

                        <td>

                          <span className="temperature-value">
                            {Number(
                              event.temperature
                            ).toFixed(1)}{" "}
                            °C
                          </span>

                        </td>

                        <td>

                          <span className="timestamp">
                            {formatTimestamp(
                              event.timestamp
                            )}
                          </span>

                        </td>

                        <td>

                          <span
                            className={
                              status.className
                            }
                          >
                            <span className="status-dot" />

                            {status.label}
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