import { DagRun } from '../hooks/usePipelineStatus'

const STATE_COLOR: Record<string, string> = {
  success: '#22c55e',
  failed:  '#ef4444',
  running: '#f59e0b',
  queued:  '#6366f1',
}

interface Props {
  runs: DagRun[]
  loading: boolean
}

export function PipelineStatusTable({ runs, loading }: Props) {
  return (
    <div className="chart-panel">
      <h2>Pipeline DAG Runs</h2>
      {loading ? (
        <p style={{ color: '#64748b' }}>Loading...</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
          <thead>
            <tr style={{ color: '#64748b', textAlign: 'left' }}>
              <th style={{ padding: '0.5rem 0.75rem' }}>DAG ID</th>
              <th style={{ padding: '0.5rem 0.75rem' }}>State</th>
              <th style={{ padding: '0.5rem 0.75rem' }}>Execution Date</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r, i) => (
              <tr key={i} style={{ borderTop: '1px solid #2d3148' }}>
                <td style={{ padding: '0.5rem 0.75rem', fontFamily: 'monospace' }}>{r.dag_id}</td>
                <td style={{ padding: '0.5rem 0.75rem' }}>
                  <span style={{ color: STATE_COLOR[r.state] ?? '#94a3b8' }}>
                    {r.state.toUpperCase()}
                  </span>
                </td>
                <td style={{ padding: '0.5rem 0.75rem', color: '#64748b' }}>{r.execution_date}</td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr>
                <td colSpan={3} style={{ padding: '1rem 0.75rem', color: '#64748b', textAlign: 'center' }}>
                  No recent DAG runs
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
