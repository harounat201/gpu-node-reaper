import { useState, useEffect } from 'react'

interface NodeResponse {
  name: string
  state: string
  cordoned: boolean
  activeSince?: string
  consolidationCandidate: boolean
  doNotDisrupt: boolean
  podCount: number
}

interface StatsResponse {
  total: number
  byState: Record<string, number>
  cordoned: number
  consolidationCandidates: number
}

const STATE_COLORS: Record<string, string> = {
  ACTIVE: '#22c55e',
  COMPLETING: '#f59e0b',
  RECLAIMABLE: '#f97316',
  RELEASED: '#60a5fa',
}

function StatTile({ label, value }: { label: string; value: number }) {
  return (
    <div className="stat-tile">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function NodeCard({ node }: { node: NodeResponse }) {
  const color = STATE_COLORS[node.state] ?? '#6b7280'

  return (
    <div className="node-card">
      <div className="node-card-header">
        <span className="node-name">{node.name}</span>
        <span className="state-pill" style={{ backgroundColor: color }}>
          {node.state || 'UNKNOWN'}
        </span>
      </div>
      <div className="node-rows">
        <div className="node-row">
          <span className="row-label">Pods</span>
          <span className="row-value">{node.podCount}</span>
        </div>
        <div className="node-row">
          <span className="row-label">Cordoned</span>
          <span className={`row-value ${node.cordoned ? 'tag-yes-warn' : 'tag-no'}`}>
            {node.cordoned ? 'Yes' : 'No'}
          </span>
        </div>
        <div className="node-row">
          <span className="row-label">Do-not-disrupt</span>
          <span className={`row-value ${node.doNotDisrupt ? 'tag-yes-active' : 'tag-no'}`}>
            {node.doNotDisrupt ? 'Yes' : 'No'}
          </span>
        </div>
        <div className="node-row">
          <span className="row-label">Consolidation candidate</span>
          <span className={`row-value ${node.consolidationCandidate ? 'tag-yes-alert' : 'tag-no'}`}>
            {node.consolidationCandidate ? 'Yes' : 'No'}
          </span>
        </div>
        {node.activeSince && (
          <div className="node-row">
            <span className="row-label">Active since</span>
            <span className="row-value tag-mono">
              {new Date(node.activeSince).toLocaleTimeString()}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

export default function App() {
  const [nodes, setNodes] = useState<NodeResponse[]>([])
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [connected, setConnected] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  async function fetchData() {
    try {
      const [nodesRes, statsRes] = await Promise.all([
        fetch('/api/v1/nodes'),
        fetch('/api/v1/stats'),
      ])
      if (!nodesRes.ok || !statsRes.ok) throw new Error('non-2xx response')
      const [nodesData, statsData]: [NodeResponse[], StatsResponse] = await Promise.all([
        nodesRes.json(),
        statsRes.json(),
      ])
      setNodes(nodesData)
      setStats(statsData)
      setConnected(true)
      setLastUpdated(new Date())
    } catch {
      setConnected(false)
    }
  }

  useEffect(() => {
    fetchData()
    const id = setInterval(fetchData, 3000)
    return () => clearInterval(id)
  }, [])

  return (
    <div>
      <header className="header">
        <div className="header-left">
          <h1>GPU Node Reaper</h1>
          <span className="header-subtitle">Karpenter · Kind dev cluster</span>
        </div>
        <div className="header-right">
          <span className={`connection-dot ${connected ? 'ok' : 'err'}`}>
            {connected ? '● Connected' : '○ Disconnected'}
          </span>
          {lastUpdated && (
            <span className="last-updated">
              Updated {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </header>

      {stats && (
        <div className="stats-bar">
          <StatTile label="Nodes" value={stats.total} />
          <StatTile label="Active" value={stats.byState['ACTIVE'] ?? 0} />
          <StatTile label="Completing" value={stats.byState['COMPLETING'] ?? 0} />
          <StatTile label="Reclaimable" value={stats.byState['RECLAIMABLE'] ?? 0} />
          <StatTile label="Released" value={stats.byState['RELEASED'] ?? 0} />
          <StatTile label="Cordoned" value={stats.cordoned} />
          <StatTile label="Consolidation" value={stats.consolidationCandidates} />
        </div>
      )}

      <main className="main">
        <div className="section-heading">GPU Nodes</div>
        {nodes.length === 0 ? (
          <div className="empty-state">
            {connected ? 'No GPU nodes found' : 'Waiting for API server at :8082…'}
          </div>
        ) : (
          <div className="node-grid">
            {nodes.map(node => (
              <NodeCard key={node.name} node={node} />
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
