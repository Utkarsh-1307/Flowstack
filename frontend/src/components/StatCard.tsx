interface Props {
  label: string
  value: string | number
  accent?: string
}

export function StatCard({ label, value, accent = '#6366f1' }: Props) {
  return (
    <div className="card" style={{ borderTop: `3px solid ${accent}` }}>
      <h3>{label}</h3>
      <div className="value">{value}</div>
    </div>
  )
}
