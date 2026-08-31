import axios from "axios";

const api = axios.create({
  baseURL: "http://localhost:8000",
  headers: {
    "Content-Type": "application/json",
  },
});

export const getTelemetry = async () => {
  const response = await api.get("/api/telemetry");
  return response.data;
};

export const getMetrics = async () => {
  const response = await api.get("/api/metrics");
  return response.data;
};

export const getHealth = async () => {
  const response = await api.get("/api/health");
  return response.data;
};

export default api;