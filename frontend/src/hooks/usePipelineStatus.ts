import { useEffect, useState } from 'react'

export interface DagRun {
  dag_id: string
  state: 'success' | 'failed' | 'running' | 'queued'
  execution_date: string
}

export function usePipelineStatus(pollMs = 30_000) {
  const [runs, setRuns] = useState<DagRun[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchRuns() {
      try {
        const res = await fetch('/api/v1/pipeline/dag-runs')
        if (res.ok) setRuns(await res.json())
      } finally {
        setLoading(false)
      }
    }
    fetchRuns()
    const id = setInterval(fetchRuns, pollMs)
    return () => clearInterval(id)
  }, [pollMs])

  return { runs, loading }
}
