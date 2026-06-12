import { useMetricsWebSocket } from './hooks/useMetricsWebSocket'
import { usePipelineStatus } from './hooks/usePipelineStatus'
import { StatCard } from './components/StatCard'
import { EventTypeBarChart } from './components/EventTypeBarChart'
import { PipelineStatusTable } from './components/PipelineStatusTable'

export default function App() {
  const metrics = useMetricsWebSocket()
  const pipeline = usePipelineStatus()

  const topEventType = Object.entries(metrics.eventCounts).sort((a, b) => b[1] - a[1])[0]?.[0] ?? '—'

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1 style={{ fontSize: '1.5rem', fontWeight: 700 }}>FlowStack Analytics</h1>
        <div style={{ fontSize: '0.875rem', color: '#94a3b8' }}>
          <span className={`status-dot ${metrics.connected ? 'live' : 'closed'}`} />
          {metrics.connected ? 'Live' : 'Reconnecting...'}
        </div>
      </header>

      <div className="cards-grid">
        <StatCard label="Total Events" value={metrics.totalEvents.toLocaleString()} accent="#6366f1" />
        <StatCard label="Event Types" value={Object.keys(metrics.eventCounts).length} accent="#22c55e" />
        <StatCard label="Top Event Type" value={topEventType} accent="#f59e0b" />
        <StatCard label="Last Event" value={metrics.lastEventType ?? '—'} accent="#06b6d4" />
      </div>

      <EventTypeBarChart eventCounts={metrics.eventCounts} />
      <PipelineStatusTable runs={pipeline.runs} loading={pipeline.loading} />
    </div>
  )
}
