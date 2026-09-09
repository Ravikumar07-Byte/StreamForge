import {
  ReactFlow,
  Background,
  MarkerType,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

const nodeStyle = (border, background) => ({
  width: 210,
  padding: "18px 16px",
  borderRadius: "10px",
  border: `1px solid ${border}`,
  background,
  color: "#f8fafc",
  boxShadow: "0 8px 25px rgba(0,0,0,0.25)",
  textAlign: "center",
});

const label = (title, subtitle) => (
  <div>
    <div
      style={{
        fontSize: "13px",
        fontWeight: 700,
        color: "#f8fafc",
      }}
    >
      {title}
    </div>

    <div
      style={{
        marginTop: "6px",
        fontSize: "10px",
        color: "#94a3b8",
      }}
    >
      {subtitle}
    </div>
  </div>
);

function Topology() {
  const nodes = [
    {
      id: "producer",
      position: { x: 350, y: 20 },
      data: {
        label: label(
          "Telemetry Producer",
          "Live truck telemetry"
        ),
      },
      style: nodeStyle("#f59e0b", "#17120a"),
    },

    {
      id: "kafka",
      position: { x: 350, y: 130 },
      data: {
        label: label(
          "Apache Kafka",
          "truck-telemetry topic"
        ),
      },
      style: nodeStyle("#f97316", "#1a120d"),
    },

    {
      id: "consumer",
      position: { x: 350, y: 240 },
      data: {
        label: label(
          "Python Consumer",
          "Kafka event consumer"
        ),
      },
      style: nodeStyle("#22c55e", "#0d1712"),
    },

    {
      id: "processing",
      position: { x: 350, y: 350 },
      data: {
        label: label(
          "Stream Processing",
          "Windows · Watermarks · Alerts"
        ),
      },
      style: nodeStyle("#38bdf8", "#0b1620"),
    },

    {
      id: "rocksdb",
      position: { x: 80, y: 490 },
      data: {
        label: label(
          "RocksDB",
          "Persistent state & recovery"
        ),
      },
      style: nodeStyle("#a78bfa", "#130f1d"),
    },

    {
      id: "fastapi",
      position: { x: 620, y: 490 },
      data: {
        label: label(
          "FastAPI",
          "REST API"
        ),
      },
      style: nodeStyle("#06b6d4", "#08171b"),
    },

    {
      id: "react",
      position: { x: 620, y: 610 },
      data: {
        label: label(
          "React Dashboard",
          "Live visualization"
        ),
      },
      style: nodeStyle("#60a5fa", "#0b1422"),
    },
  ];

  const edges = [
    {
      id: "producer-kafka",
      source: "producer",
      target: "kafka",
      animated: true,
      style: {
        stroke: "#f59e0b",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#f59e0b",
      },
    },

    {
      id: "kafka-consumer",
      source: "kafka",
      target: "consumer",
      animated: true,
      style: {
        stroke: "#f97316",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#f97316",
      },
    },

    {
      id: "consumer-processing",
      source: "consumer",
      target: "processing",
      animated: true,
      style: {
        stroke: "#22c55e",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#22c55e",
      },
    },

    {
      id: "processing-rocksdb",
      source: "processing",
      target: "rocksdb",
      animated: true,
      style: {
        stroke: "#a78bfa",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#a78bfa",
      },
    },

    {
      id: "processing-fastapi",
      source: "processing",
      target: "fastapi",
      animated: true,
      style: {
        stroke: "#06b6d4",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#06b6d4",
      },
    },

    {
      id: "fastapi-react",
      source: "fastapi",
      target: "react",
      animated: true,
      style: {
        stroke: "#60a5fa",
        strokeWidth: 2,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: "#60a5fa",
      },
    },
  ];

  return (
    <main
      style={{
        minHeight: "100vh",
        padding: "28px",
        background: "#0f172a",
        color: "#e5e7eb",
      }}
    >
      {/* Header */}
      <section style={{ marginBottom: "24px" }}>
        <div
          style={{
            fontSize: "11px",
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "#38bdf8",
            marginBottom: "7px",
          }}
        >
          SYSTEM ARCHITECTURE
        </div>

        <h1
          style={{
            margin: 0,
            fontSize: "26px",
            fontWeight: 700,
            color: "#f8fafc",
          }}
        >
          Stream Topology
        </h1>

        <p
          style={{
            margin: "7px 0 0",
            color: "#64748b",
            fontSize: "13px",
          }}
        >
          Real-time data flow across the StreamForge platform
        </p>
      </section>

      {/* Pipeline */}
      <section
        style={{
          border: "1px solid #243244",
          borderRadius: "8px",
          overflow: "hidden",
          background: "#0b1220",
        }}
      >
        <div
          style={{
            padding: "15px 18px",
            borderBottom: "1px solid #243244",
            background: "#101a28",
          }}
        >
          <h2
            style={{
              margin: 0,
              fontSize: "14px",
              color: "#f8fafc",
            }}
          >
            Real-Time Data Pipeline
          </h2>

          <p
            style={{
              margin: "5px 0 0",
              fontSize: "11px",
              color: "#64748b",
            }}
          >
            Producer → Kafka → Consumer → Processing → Storage / API → Dashboard
          </p>
        </div>

        <div
          style={{
            height: "760px",
            width: "100%",
          }}
        >
          <ReactFlow
            nodes={nodes}
            edges={edges}
            fitView
            fitViewOptions={{
              padding: 0.15,
              maxZoom: 0.9,
              minZoom: 0.55,
            }}
            minZoom={0.45}
            maxZoom={1.2}
            nodesDraggable={true}
            nodesConnectable={false}
            elementsSelectable={true}
            proOptions={{
              hideAttribution: true,
            }}
          >
            <Background
              color="#1e293b"
              gap={24}
              size={1}
            />
          </ReactFlow>
        </div>
      </section>

      {/* Architecture Layers */}
      <section style={{ marginTop: "18px" }}>
        <div
          style={{
            border: "1px solid #243244",
            borderRadius: "8px",
            overflow: "hidden",
            background: "#0b1220",
          }}
        >
          <div
            style={{
              padding: "15px 18px",
              borderBottom: "1px solid #243244",
              background: "#101a28",
            }}
          >
            <h2
              style={{
                margin: 0,
                fontSize: "14px",
                color: "#f8fafc",
              }}
            >
              Architecture Layers
            </h2>

            <p
              style={{
                margin: "5px 0 0",
                fontSize: "11px",
                color: "#64748b",
              }}
            >
              Core technologies powering StreamForge
            </p>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns:
                "repeat(5, minmax(0, 1fr))",
            }}
          >
            {[
              ["STREAMING", "Apache Kafka"],
              ["PROCESSING", "Python Consumer"],
              ["STATE", "RocksDB"],
              ["API", "FastAPI"],
              ["FRONTEND", "React"],
            ].map(([category, technology]) => (
              <div
                key={category}
                style={{
                  padding: "16px",
                  borderRight: "1px solid #243244",
                }}
              >
                <div
                  style={{
                    fontSize: "9px",
                    fontWeight: 700,
                    letterSpacing: "0.1em",
                    color: "#64748b",
                    marginBottom: "6px",
                  }}
                >
                  {category}
                </div>

                <div
                  style={{
                    fontSize: "12px",
                    fontWeight: 600,
                    color: "#e2e8f0",
                  }}
                >
                  {technology}
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}

export default Topology;