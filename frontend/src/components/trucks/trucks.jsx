import { useCallback, useEffect, useMemo, useState } from "react";
import {
  CarFront,
  RefreshCw,
  Thermometer,
  Wifi,
  WifiOff,
} from "lucide-react";

import api from "../../services/api";

const REFRESH_INTERVAL = 5000;
const WARNING_TEMPERATURE = 35;

function Trucks() {
  const [trucks, setTrucks] = useState([]);
  const [activeTruckCount, setActiveTruckCount] = useState(0);
  const [selectedTruck, setSelectedTruck] = useState(null);
  const [kafkaStatus, setKafkaStatus] = useState("Checking");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const fetchTruckData = useCallback(async () => {
    try {
      const [telemetryResponse, metricsResponse] =
        await Promise.all([
          api.get("/api/telemetry"),
          api.get("/api/metrics"),
        ]);

      const telemetry = Array.isArray(
        telemetryResponse.data.telemetry
      )
        ? telemetryResponse.data.telemetry
        : [];

      const metrics = metricsResponse.data || {};

      setKafkaStatus(
        telemetryResponse.data.kafka_status || "Unknown"
      );

      setActiveTruckCount(
        Number(metrics.active_trucks || 0)
      );

      const activeTruckIds = new Set([
        "TRUCK-000001",
        "TRUCK-000002",
        "TRUCK-000003",
        "TRUCK-000004",
        "TRUCK-000005",
      ]);

      const latestByTruck = new Map();

      telemetry
        .filter((event) => activeTruckIds.has(event.truck))
        .forEach((event) => {
          if (!event.truck) return;

          const existing = latestByTruck.get(event.truck);

          if (
            !existing ||
            new Date(event.timestamp) >
              new Date(existing.timestamp)
          ) {
            latestByTruck.set(event.truck, event);
          }
        });

      const liveTrucks = Array.from(
        latestByTruck.values()
      )
        .map((event) => ({
          id: event.truck,
          temperature: Number(event.temperature),
          timestamp: event.timestamp,
        }))
        .sort((a, b) => a.id.localeCompare(b.id));

      setTrucks(liveTrucks);
      setError("");
    } catch (err) {
      console.error("Trucks API error:", err);

      setKafkaStatus("Offline");
      setError(
        "Unable to load live truck telemetry."
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTruckData();

    const interval = setInterval(
      fetchTruckData,
      REFRESH_INTERVAL
    );

    return () => clearInterval(interval);
  }, [fetchTruckData]);

  const warningCount = useMemo(() => {
    return trucks.filter(
      (truck) =>
        truck.temperature >= WARNING_TEMPERATURE
    ).length;
  }, [trucks]);

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

  if (loading) {
    return (
      <main className="dashboard-page">
        <div className="dashboard-loading">
          <RefreshCw
            className="spin"
            size={30}
          />

          <h2>Loading Fleet</h2>

          <p>
            Connecting to live truck telemetry...
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
            <CarFront size={15} />
            FLEET MONITORING
          </div>

          <h1>Trucks</h1>

          <p>
            Monitor the current status of your truck fleet
          </p>
        </div>

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
            Fleet {isOnline ? "Online" : "Offline"}
          </span>
        </div>
      </section>

      {error && (
        <div className="dashboard-error">
          {error}
        </div>
      )}

      <section className="kpi-grid">
        <div className="kpi-card blue-card">
          <div className="kpi-icon">
            <CarFront size={22} />
          </div>

          <div className="kpi-content">
            <span>Total Trucks</span>
            <strong>{activeTruckCount}</strong>
            <small>Currently active vehicles</small>
          </div>
        </div>

        <div className="kpi-card green-card">
          <div className="kpi-icon">
            <Wifi size={22} />
          </div>

          <div className="kpi-content">
            <span>Online</span>
            <strong>{trucks.length}</strong>
            <small>Currently reporting</small>
          </div>
        </div>

        <div className="kpi-card orange-card">
          <div className="kpi-icon">
            <Thermometer size={22} />
          </div>

          <div className="kpi-content">
            <span>Temperature Warning</span>
            <strong>{warningCount}</strong>
            <small>Above 35°C threshold</small>
          </div>
        </div>
      </section>

      <section className="dashboard-panel">
        <div className="panel-title">
          <div>
            <h2>Truck Fleet</h2>
            <p>Latest vehicle telemetry status</p>
          </div>

          <span className="event-count">
            {trucks.length} vehicles
          </span>
        </div>

        <div className="table-wrapper">
          {trucks.length === 0 ? (
            <div className="empty-state">
              <CarFront size={30} />

              <h3>No active trucks</h3>

              <p>
                Waiting for live telemetry from Kafka.
              </p>
            </div>
          ) : (
            <table className="modern-table">
              <thead>
                <tr>
                  <th>Truck</th>
                  <th>Temperature</th>
                  <th>Connection</th>
                  <th>Status</th>
                </tr>
              </thead>

              <tbody>
                {trucks.map((truck) => {
                  const warning =
                    truck.temperature >=
                    WARNING_TEMPERATURE;

                  const isSelected =
                    selectedTruck?.id === truck.id;

                  return (
                    <tr
                      key={truck.id}
                      onClick={() =>
                        setSelectedTruck(truck)
                      }
                      style={{
                        cursor: "pointer",
                        background: isSelected
                          ? "rgba(59, 130, 246, 0.08)"
                          : undefined,
                      }}
                    >
                      <td>
                        <div className="truck-info">
                          <div className="truck-icon">
                            <CarFront size={16} />
                          </div>

                          <strong>
                            {truck.id}
                          </strong>
                        </div>
                      </td>

                      <td>
                        <strong
                          className={
                            warning
                              ? "temperature-warning"
                              : "temperature-normal"
                          }
                        >
                          {truck.temperature.toFixed(1)}°C
                        </strong>
                      </td>

                      <td>
                        <span className="status-badge normal">
                          <span />
                          Online
                        </span>
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
          )}
        </div>
      </section>

      {selectedTruck && (
        <section className="dashboard-panel">
          <div className="panel-title">
            <div>
              <h2>Truck Details</h2>
              <p>
                Current telemetry for{" "}
                {selectedTruck.id}
              </p>
            </div>

            <CarFront size={22} />
          </div>

          <div className="health-main">
            <div className="health-row">
              <span>Truck ID</span>
              <strong>
                {selectedTruck.id}
              </strong>
            </div>

            <div className="health-row">
              <span>Temperature</span>
              <strong
                className={
                  selectedTruck.temperature >=
                  WARNING_TEMPERATURE
                    ? "temperature-warning"
                    : "temperature-normal"
                }
              >
                {selectedTruck.temperature.toFixed(1)}°C
              </strong>
            </div>

            <div className="health-row">
              <span>Connection</span>
              <strong className="green-text">
                Online
              </strong>
            </div>

            <div className="health-row">
              <span>Status</span>
              <strong
                className={
                  selectedTruck.temperature >=
                  WARNING_TEMPERATURE
                    ? "temperature-warning"
                    : "green-text"
                }
              >
                {selectedTruck.temperature >=
                WARNING_TEMPERATURE
                  ? "Warning"
                  : "Normal"}
              </strong>
            </div>

            <div className="health-row">
              <span>Last Telemetry</span>
              <strong>
                {formatTimestamp(
                  selectedTruck.timestamp
                )}
              </strong>
            </div>
          </div>
        </section>
      )}
    </main>
  );
}

export default Trucks;