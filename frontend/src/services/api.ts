import { TelemetryFrame, SimulationConfig, BenchmarkRequest, BenchmarkResult, ExperimentRecord } from '../types';

const API_BASE = 'http://127.0.0.1:8000/api';
const WS_URL = 'ws://127.0.0.1:8000/ws/telemetry';

export async function fetchLatestTelemetry(): Promise<TelemetryFrame> {
  const res = await fetch(`${API_BASE}/telemetry/latest`);
  if (!res.ok) throw new Error('Failed to fetch telemetry');
  return res.json();
}

export async function resetSimulation(): Promise<TelemetryFrame> {
  const res = await fetch(`${API_BASE}/telemetry/reset`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to reset simulation');
  return res.json();
}

export async function fetchSimulationConfig(): Promise<SimulationConfig> {
  const res = await fetch(`${API_BASE}/simulation/config`);
  if (!res.ok) throw new Error('Failed to fetch config');
  return res.json();
}

export async function updateSimulationConfig(cfg: Partial<SimulationConfig>): Promise<SimulationConfig> {
  const res = await fetch(`${API_BASE}/simulation/config`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(cfg),
  });
  if (!res.ok) throw new Error('Failed to update config');
  return res.json();
}

export async function toggleAnchor(anchorId: string, status: string): Promise<any> {
  const res = await fetch(`${API_BASE}/anchors/${anchorId}/status?status=${status}`, {
    method: 'POST',
  });
  if (!res.ok) throw new Error('Failed to toggle anchor');
  return res.json();
}

export async function runBenchmark(req: BenchmarkRequest): Promise<BenchmarkResult> {
  const res = await fetch(`${API_BASE}/benchmark/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) throw new Error('Benchmark failed');
  return res.json();
}

export async function fetchExperiments(): Promise<ExperimentRecord[]> {
  const res = await fetch(`${API_BASE}/experiments`);
  if (!res.ok) throw new Error('Failed to fetch experiments');
  return res.json();
}

export async function fetchExperimentDetails(id: number): Promise<any> {
  const res = await fetch(`${API_BASE}/experiments/${id}`);
  if (!res.ok) throw new Error('Failed to fetch experiment details');
  return res.json();
}

export async function deleteExperiment(id: number): Promise<any> {
  const res = await fetch(`${API_BASE}/experiments/${id}`, { method: 'DELETE' });
  if (!res.ok) throw new Error('Failed to delete experiment');
  return res.json();
}

export function getExportCsvUrl(id: number): string {
  return `${API_BASE}/experiments/${id}/export/csv`;
}

export function getExportJsonUrl(id: number): string {
  return `${API_BASE}/experiments/${id}/export/json`;
}

export class TelemetryWebSocketClient {
  private ws: WebSocket | null = null;
  private onMessageCallback: (frame: TelemetryFrame) => void;
  private onStatusChange: (connected: boolean) => void;
  private reconnectTimer: any = null;
  private isDestroyed = false;

  constructor(
    onMessage: (frame: TelemetryFrame) => void,
    onStatus: (connected: boolean) => void
  ) {
    this.onMessageCallback = onMessage;
    this.onStatusChange = onStatus;
    this.connect();
  }

  public connect() {
    if (this.isDestroyed) return;
    try {
      this.ws = new WebSocket(WS_URL);
      this.ws.onopen = () => {
        this.onStatusChange(true);
      };
      this.ws.onmessage = (event) => {
        try {
          const frame: TelemetryFrame = JSON.parse(event.data);
          this.onMessageCallback(frame);
        } catch (e) {
          console.error('Error parsing telemetry frame:', e);
        }
      };
      this.ws.onclose = () => {
        this.onStatusChange(false);
        if (!this.isDestroyed) {
          this.reconnectTimer = setTimeout(() => this.connect(), 2000);
        }
      };
      this.ws.onerror = () => {
        this.onStatusChange(false);
      };
    } catch (e) {
      this.onStatusChange(false);
      if (!this.isDestroyed) {
        this.reconnectTimer = setTimeout(() => this.connect(), 2000);
      }
    }
  }

  public send(data: Record<string, any>) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  public destroy() {
    this.isDestroyed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
