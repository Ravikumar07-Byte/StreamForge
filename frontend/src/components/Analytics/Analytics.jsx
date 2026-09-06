import { useCallback, useEffect, useMemo, useState } from "react";

import {
  Activity,
  AlertTriangle,
  BarChart3,
  CarFront,
  RefreshCw,
  Thermometer,
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

const FLEET_TRUCKS = [
  "TRUCK-000001",
  "TRUCK-000002",
  "TRUCK-000003",
  "TRUCK-000004",
  "TRUCK-000005",
];

function Analytics() {
  const [telemetry, setTelemetry] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const fetchAnalytics = useCallback(async () => {
    try {
      const [telemetryResponse, metricsResponse] =
        await Promise.all([
          api.get("/api/telemetry"),
          api.get("/api/metrics"),
        ]);

      const telemetryData = Array.isArray(
        telemetryResponse.data.telemetry
      )
        ? telemetryResponse.data.telemetry
        : [];

      const fleetTelemetry = telemetryData.filter((event) =>
        FLEET_TRUCKS.includes(event.truck)
      );

      setTelemetry(fleetTelemetry);
      setMetrics(metricsResponse.data || {});
      setError("");
    } catch (err) {
      console.error("Analytics API error:", err);
      setError("Unable to load analytics data.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAnalytics();

    const interval = setInterval(
      fetchAnalytics,
      REFRESH_INTERVAL
    );

    return () => clearInterval(interval);
  }, [fetchAnalytics]);

  const temperatures = useMemo(() => {
    return telemetry
      .map((event) => Number(event.temperature))
      .filter((value) => Number.isFinite(value));
  }, [telemetry]);

  const averageTemperature = useMemo(() => {
    if (!temperatures.length) return "--";

    return (
      temperatures.reduce(
        (sum, value) => sum + value,
        0
      ) / temperatures.length
    ).toFixed(1);
  }, [temperatures]);

  const highestTemperature = useMemo(() => {
    if (!temperatures.length) return "--";

    return Math.max(...temperatures).toFixed(1);
  }, [temperatures]);

  const lowestTemperature = useMemo(() => {
    if (!temperatures.length) return "--";

    return Math.min(...temperatures).toFixed(1);
  }, [temperatures]);

  const warningCount = useMemo(() => {
    return temperatures.filter(
      (temperature) =>
        temperature >= WARNING_TEMPERATURE
    ).length;
  }, [temperatures]);

  const truckStats = useMemo(() => {
    return FLEET_TRUCKS.map((truckId) => {
      const truckEvents = telemetry.filter(
        (event) => event.truck === truckId
      );

      const validEvents = truckEvents.filter((event) =>
        Number.isFinite(Number(event.temperature))
      );

      if (!validEvents.length) {
        return {
          truck: truckId,
          events: 0,
          averageTemperature: "--",
          latestTemperature: null,
          latestTimestamp: null,
        };
      }

      const sortedEvents = [...validEvents].sort(
        (a, b) =>
          new Date(b.timestamp) -
          new Date(a.timestamp)
      );

      const totalTemperature = validEvents.reduce(
        (sum, event) =>
          sum + Number(event.temperature),
        0
      );

      return {
        truck: truckId,
        events: validEvents.length,
        averageTemperature: (
          totalTemperature / validEvents.length
        ).toFixed(1),
        latestTemperature: Number(
          sortedEvents[0].temperature
        ),
        latestTimestamp:
          sortedEvents[0].timestamp,
      };
    });
  }, [telemetry]);

  const chartData = useMemo(() => {
    return telemetry
      .slice(-30)
      .map((event) => {
        const date = new Date(event.timestamp);

        return {
          time: Number.isNaN(date.getTime())
            ? "--"
            : date.toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              }),
          temperature: Number(event.temperature),
          truck: event.truck,
        };
      });
  }, [telemetry]);

  if (loading) {
    return (
      <main className="dashboard-page">
        <div className="dashboard-loading">
          <RefreshCw
            className="spin"
            size={30}
          />

          <h2>Loading Analytics</h2>

          <p>
            Analyzing live truck telemetry...
          </p>
        </div>
      </main>
    );
  }

  return (
    <main className="dashboard-page">
      <section className="dashboard-topbar">
        <div>
          <div className="dashboard-eyebrow">
            <BarChart3 size={15} />
            TELEMETRY ANALYTICS
          </div>

          <h1>Analytics</h1>

          <p>
            Analyze truck telemetry and temperature trends
          </p>
        </div>
      </section>

      {error && (
        <div className="dashboard-error">
          <AlertTriangle size={18} />
          {error}
        </div>
      )}

      {/* ANALYTICS KPIs */}
      <section className="kpi-grid">
        <div className="kpi-card blue-card">
          <div className="kpi-icon">
            <Activity size={22} />
          </div>

          <div className="kpi-content">
            <span>Total Events</span>

            <strong>
              {Number(
                metrics.events_received || 0
              ).toLocaleString()}
            </strong>

            <small>
              Events received by the platform
            </small>
          </div>
        </div>

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
              Current fleet telemetry
            </small>
          </div>
        </div>

        <div className="kpi-card red-card">
          <div className="kpi-icon">
            <Thermometer size={22} />
          </div>

          <div className="kpi-content">
            <span>Highest Temperature</span>

            <strong>
              {highestTemperature}
              <em>°C</em>
            </strong>

            <small>
              Highest current fleet value
            </small>
          </div>
        </div>

        <div className="kpi-card green-card">
          <div className="kpi-icon">
            <CarFront size={22} />
          </div>

          <div className="kpi-content">
            <span>Active Trucks</span>

            <strong>
              {Number(
                metrics.active_trucks || 0
              )}
            </strong>

            <small>
              Currently reporting
            </small>
          </div>
        </div>
      </section>

      {/* TEMPERATURE TREND */}
      <section className="dashboard-panel">
        <div className="panel-title">
          <div>
            <div className="title-row">
              <h2>Temperature Trend</h2>

              <span className="live-badge">
                <span />
                LIVE
              </span>
            </div>

            <p>
              Latest temperature measurements
            </p>
          </div>

          <span className="panel-meta">
            Last 30 events
          </span>
        </div>

        <div className="chart-area">
          {chartData.length === 0 ? (
            <div className="empty-state">
              <Thermometer size={34} />

              <h3>No telemetry data</h3>

              <p>
                Waiting for telemetry events.
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
      </section>

      {/* TEMPERATURE + PROCESSING */}
      <section className="content-grid">
        <div className="dashboard-panel health-panel">
          <div className="panel-title">
            <div>
              <h2>Temperature Range</h2>

              <p>
                Current fleet temperature values
              </p>
            </div>

            <Thermometer size={22} />
          </div>

          <div className="health-main">
            <div className="health-row">
              <span>Average</span>

              <strong>
                {averageTemperature}°C
              </strong>
            </div>

            <div className="health-row">
              <span>Highest</span>

              <strong className="temperature-warning">
                {highestTemperature}°C
              </strong>
            </div>

            <div className="health-row">
              <span>Lowest</span>

              <strong>
                {lowestTemperature}°C
              </strong>
            </div>

            <div className="health-row">
              <span>Warning Readings</span>

              <strong className="temperature-warning">
                {warningCount}
              </strong>
            </div>
          </div>
        </div>

        <div className="dashboard-panel health-panel">
          <div className="panel-title">
            <div>
              <h2>Processing Metrics</h2>

              <p>
                Stream processing activity
              </p>
            </div>

            <Activity size={22} />
          </div>

          <div className="health-main">
            <div className="health-row">
              <span>Events Received</span>

              <strong>
                {Number(
                  metrics.events_received || 0
                ).toLocaleString()}
              </strong>
            </div>

            <div className="health-row">
              <span>Events Processed</span>

              <strong className="green-text">
                {Number(
                  metrics.events_processed || 0
                ).toLocaleString()}
              </strong>
            </div>

            <div className="health-row">
              <span>Invalid Events</span>

              <strong>
                {Number(
                  metrics.events_invalid || 0
                ).toLocaleString()}
              </strong>
            </div>

            <div className="health-row">
              <span>Late Events</span>

              <strong>
                {Number(
                  metrics.events_late || 0
                ).toLocaleString()}
              </strong>
            </div>
          </div>
        </div>
      </section>

      {/* TRUCK ANALYTICS */}
      <section className="dashboard-panel">
        <div className="panel-title">
          <div>
            <h2>Truck Analytics</h2>

            <p>
              Event and temperature statistics by truck
            </p>
          </div>

          <span className="event-count">
            {FLEET_TRUCKS.length} trucks
          </span>
        </div>

        <div className="table-wrapper">
          <table className="modern-table">
            <thead>
              <tr>
                <th>Truck</th>
                <th>Events</th>
                <th>Average Temperature</th>
                <th>Latest Temperature</th>
                <th>Status</th>
              </tr>
            </thead>

            <tbody>
              {truckStats.map((truck) => {
                const warning =
                  truck.latestTemperature !== null &&
                  truck.latestTemperature >=
                    WARNING_TEMPERATURE;

                return (
                  <tr key={truck.truck}>
                    <td>
                      <div className="truck-info">
                        <div className="truck-icon">
                          <CarFront size={16} />
                        </div>

                        <strong>
                          {truck.truck}
                        </strong>
                      </div>
                    </td>

                    <td>
                      {truck.events}
                    </td>

                    <td>
                      {truck.averageTemperature ===
                      "--"
                        ? "--"
                        : `${truck.averageTemperature}°C`}
                    </td>

                    <td>
                      {truck.latestTemperature ===
                      null ? (
                        "--"
                      ) : (
                        <strong
                          className={
                            warning
                              ? "temperature-warning"
                              : "temperature-normal"
                          }
                        >
                          {truck.latestTemperature.toFixed(
                            1
                          )}
                          °C
                        </strong>
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
              })}
            </tbody>
          </table>
        </div>
      </section>

      <footer className="dashboard-footer">
        © 2026 StreamForge. Real-time Truck Telemetry Platform.
      </footer>
    </main>
  );
}

export default Analytics;