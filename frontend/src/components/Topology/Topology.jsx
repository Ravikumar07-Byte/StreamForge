import {
  Activity,
  Database,
  Gauge,
  Radio,
  Server,
  Workflow,
} from "lucide-react";

function Topology() {
  const nodes = [
    {
      title: "Telemetry Producer",
      description: "Generates live truck telemetry",
      icon: Radio,
      status: "Running",
    },
    {
      title: "Apache Kafka",
      description: "Streams truck telemetry events",
      icon: Activity,
      status: "Online",
    },
    {
      title: "Telemetry Consumer",
      description: "Consumes and validates events",
      icon: Workflow,
      status: "Running",
    },
    {
      title: "Stream Processing",
      description: "Windows, aggregation and alerts",
      icon: Gauge,
      status: "Running",
    },
    {
      title: "RocksDB",
      description: "Stores persistent application state",
      icon: Database,
      status: "Online",
    },
    {
      title: "FastAPI",
      description: "Provides telemetry REST APIs",
      icon: Server,
      status: "Online",
    },
    {
      title: "React Dashboard",
      description: "Displays live telemetry data",
      icon: Workflow,
      status: "Online",
    },
  ];

  return (
    <main className="dashboard-page">
      <section className="dashboard-topbar">
        <div>
          <div className="dashboard-eyebrow">
            <Workflow size={15} />
            SYSTEM ARCHITECTURE
          </div>

          <h1>Stream Topology</h1>

          <p>
            Real-time data flow across the StreamForge platform
          </p>
        </div>
      </section>

      <section className="dashboard-panel">
        <div className="panel-title">
          <div>
            <h2>Real-Time Data Pipeline</h2>

            <p>
              Telemetry flow from producer to dashboard
            </p>
          </div>

          <span className="live-badge">
            <span />
            LIVE
          </span>
        </div>

        <div className="topology-container">
          {nodes.map((node, index) => {
            const Icon = node.icon;

            return (
              <div
                className="topology-step"
                key={node.title}
              >
                <div className="topology-node">
                  <div className="topology-icon">
                    <Icon size={24} />
                  </div>

                  <div className="topology-info">
                    <h3>{node.title}</h3>

                    <p>{node.description}</p>

                    <span className="topology-status">
                      <span />
                      {node.status}
                    </span>
                  </div>
                </div>

                {index < nodes.length - 1 && (
                  <div className="topology-arrow">
                    ↓
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </section>

      <section className="dashboard-panel">
        <div className="panel-title">
          <div>
            <h2>Architecture Overview</h2>

            <p>
              Core StreamForge system layers
            </p>
          </div>
        </div>

        <div className="health-main">
          <div className="health-row">
            <span>Streaming Layer</span>
            <strong className="green-text">
              Apache Kafka
            </strong>
          </div>

          <div className="health-row">
            <span>Processing Layer</span>
            <strong className="green-text">
              Python Consumer
            </strong>
          </div>

          <div className="health-row">
            <span>State Layer</span>
            <strong className="green-text">
              RocksDB
            </strong>
          </div>

          <div className="health-row">
            <span>API Layer</span>
            <strong className="green-text">
              FastAPI
            </strong>
          </div>

          <div className="health-row">
            <span>Presentation Layer</span>
            <strong className="green-text">
              React
            </strong>
          </div>
        </div>
      </section>
    </main>
  );
}

export default Topology;