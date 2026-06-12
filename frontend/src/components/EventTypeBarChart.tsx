import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

interface Props {
  eventCounts: Record<string, number>
}

const COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#06b6d4']

export function EventTypeBarChart({ eventCounts }: Props) {
  const data = Object.entries(eventCounts).map(([name, count]) => ({ name, count }))

  if (data.length === 0) {
    return (
      <div className="chart-panel">
        <h2>Event Type Distribution</h2>
        <p style={{ color: '#64748b', textAlign: 'center', padding: '2rem' }}>
          Waiting for events...
        </p>
      </div>
    )
  }

  return (
    <div className="chart-panel">
      <h2>Event Type Distribution</h2>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <XAxis dataKey="name" stroke="#64748b" tick={{ fill: '#94a3b8', fontSize: 12 }} />
          <YAxis stroke="#64748b" tick={{ fill: '#94a3b8', fontSize: 12 }} />
          <Tooltip
            contentStyle={{ background: '#1e2130', border: '1px solid #2d3148', borderRadius: 8 }}
            labelStyle={{ color: '#e2e8f0' }}
          />
          <Bar dataKey="count" radius={[4, 4, 0, 0]}>
            {data.map((_, i) => (
              <Cell key={i} fill={COLORS[i % COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
