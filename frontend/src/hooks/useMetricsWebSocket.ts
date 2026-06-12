import { useEffect, useRef, useState, useCallback } from 'react'

export interface WsEvent {
  type: string
  eventType: string
  offset: number | null
}

export interface MetricsState {
  totalEvents: number
  eventCounts: Record<string, number>
  lastEventType: string | null
  connected: boolean
}

const WS_URL = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/metrics`

export function useMetricsWebSocket(): MetricsState {
  const ws = useRef<WebSocket | null>(null)
  const [state, setState] = useState<MetricsState>({
    totalEvents: 0,
    eventCounts: {},
    lastEventType: null,
    connected: false,
  })

  const connect = useCallback(() => {
    ws.current = new WebSocket(WS_URL)

    ws.current.onopen = () =>
      setState(prev => ({ ...prev, connected: true }))

    ws.current.onmessage = (e: MessageEvent) => {
      const msg: WsEvent = JSON.parse(e.data)
      if (msg.type === 'new_event') {
        setState(prev => ({
          ...prev,
          totalEvents: prev.totalEvents + 1,
          lastEventType: msg.eventType,
          eventCounts: {
            ...prev.eventCounts,
            [msg.eventType]: (prev.eventCounts[msg.eventType] ?? 0) + 1,
          },
        }))
      }
    }

    ws.current.onclose = () => {
      setState(prev => ({ ...prev, connected: false }))
      // Auto-reconnect after 3 s
      setTimeout(connect, 3000)
    }

    ws.current.onerror = () => ws.current?.close()
  }, [])

  useEffect(() => {
    connect()
    return () => ws.current?.close()
  }, [connect])

  return state
}
