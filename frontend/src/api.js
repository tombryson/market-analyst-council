/**
 * API client for the LLM Council backend.
 */

const DEFAULT_API_BASE =
  typeof window !== 'undefined'
    ? window.location.origin
    : 'http://localhost:8001';
const API_BASE = import.meta.env.VITE_API_BASE || DEFAULT_API_BASE;
const RETRYABLE_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function readJsonSafe(response) {
  try {
    return await response.json();
  } catch (_) {
    return {};
  }
}

function errorDetailFromPayload(payload) {
  if (!payload || typeof payload !== 'object') return '';
  const detail = payload.detail;
  if (!detail) return '';
  return typeof detail === 'string' ? detail : JSON.stringify(detail);
}

async function fetchJsonWithRetry(url, init = {}, options = {}) {
  const retries = Math.max(0, Number.isFinite(Number(options.retries)) ? Number(options.retries) : 2);
  const timeoutMs = Math.max(1000, Number.isFinite(Number(options.timeoutMs)) ? Number(options.timeoutMs) : 25000);
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      clearTimeout(timeout);
      const body = await readJsonSafe(response);
      if (response.ok || !RETRYABLE_HTTP_STATUSES.has(response.status) || attempt >= retries) {
        return { response, body };
      }
      await sleep(500 * (attempt + 1));
    } catch (err) {
      clearTimeout(timeout);
      lastError = err;
      if (attempt >= retries) break;
      await sleep(500 * (attempt + 1));
    }
  }
  throw lastError instanceof Error ? lastError : new Error('Network request failed');
}

export const api = {
  /**
   * List all available analysis templates.
   */
  async listTemplates() {
    const response = await fetch(`${API_BASE}/api/templates`);
    if (!response.ok) {
      throw new Error('Failed to list templates');
    }
    return response.json();
  },

  /**
   * List all available company types.
   */
  async listCompanyTypes() {
    const response = await fetch(`${API_BASE}/api/company-types`);
    if (!response.ok) {
      throw new Error('Failed to list company types');
    }
    return response.json();
  },

  /**
   * List all available exchanges.
   */
  async listExchanges() {
    const response = await fetch(`${API_BASE}/api/exchanges`);
    if (!response.ok) {
      throw new Error('Failed to list exchanges');
    }
    return response.json();
  },

  /**
   * List recent Stage 3 run artifacts suitable for gantt-lab.
   */
  async listGanttRuns(limit = 20, ticker = '') {
    const qs = new URLSearchParams();
    qs.set('limit', String(limit));
    if (String(ticker || '').trim()) {
      qs.set('ticker', String(ticker || '').trim());
    }
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs?${qs.toString()}`,
      { method: 'GET' },
      { retries: 3, timeoutMs: 30000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to list gantt runs: ${detail}` : 'Failed to list gantt runs');
    }
    return body;
  },

  /**
   * Delete one Stage 3 run artifact family.
   */
  async deleteGanttRun(runId) {
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs/${encodeURIComponent(runId)}`,
      { method: 'DELETE' },
      { retries: 2, timeoutMs: 30000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to delete gantt run: ${detail}` : 'Failed to delete gantt run');
    }
    return body;
  },

  /**
   * Load one Stage 3 run artifact for gantt-lab.
   */
  async getGanttRun(runId) {
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs/${encodeURIComponent(runId)}`,
      { method: 'GET' },
      { retries: 3, timeoutMs: 30000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to load gantt run: ${detail}` : 'Failed to load gantt run');
    }
    return body;
  },

  /**
   * Load one integration-ready report packet for a run.
   */
  async getRunReportPacket(runId) {
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs/${encodeURIComponent(runId)}/report-packet`,
      { method: 'GET' },
      { retries: 3, timeoutMs: 35000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to load report packet: ${detail}` : 'Failed to load report packet');
    }
    return body;
  },

  /**
   * Submit an async analysis job.
   */
  async createAnalysisJob(payload) {
    const response = await fetch(`${API_BASE}/api/analysis-jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
    });
    if (!response.ok) {
      throw new Error('Failed to create analysis job');
    }
    return response.json();
  },

  /**
   * List async analysis jobs.
   */
  async listAnalysisJobs(limit = 20) {
    const response = await fetch(
      `${API_BASE}/api/analysis-jobs?limit=${encodeURIComponent(limit)}`
    );
    if (!response.ok) {
      throw new Error('Failed to list analysis jobs');
    }
    return response.json();
  },

  /**
   * Get one async analysis job status.
   */
  async getAnalysisJob(jobId) {
    const response = await fetch(
      `${API_BASE}/api/analysis-jobs/${encodeURIComponent(jobId)}`
    );
    if (!response.ok) {
      throw new Error('Failed to load analysis job');
    }
    return response.json();
  },

  /**
   * Get completed result for an async analysis job.
   */
  async getAnalysisJobResult(jobId) {
    const response = await fetch(
      `${API_BASE}/api/analysis-jobs/${encodeURIComponent(jobId)}/result`
    );
    if (!response.ok) {
      throw new Error('Failed to load analysis job result');
    }
    return response.json();
  },

  /**
   * Subscribe to live async analysis job events (SSE).
   * Returns EventSource; caller should call `close()` when finished.
   */
  streamAnalysisJobEvents(jobId, handlers = {}) {
    const source = new EventSource(
      `${API_BASE}/api/analysis-jobs/${encodeURIComponent(jobId)}/events`
    );

    const onUpdate =
      typeof handlers.onUpdate === 'function' ? handlers.onUpdate : () => {};
    const onError =
      typeof handlers.onError === 'function' ? handlers.onError : () => {};

    source.addEventListener('analysis_job', (event) => {
      try {
        const payload = JSON.parse(event.data || '{}');
        onUpdate(payload);
      } catch (err) {
        onError(err);
      }
    });

    source.addEventListener('error', (event) => {
      onError(event);
    });

    return source;
  },

  /**
   * Load latest delta-check result for one run.
   */
  async getLatestDeltaCheck(runId) {
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs/${encodeURIComponent(runId)}/delta-check/latest`,
      { method: 'GET' },
      { retries: 2, timeoutMs: 30000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to load latest delta-check: ${detail}` : 'Failed to load latest delta-check');
    }
    return body;
  },

  /**
   * Run (or fetch cached) delta-check for one run.
   */
  async runDeltaCheck(runId, options = {}) {
    const force = options.force ? 'true' : 'false';
    const maxSources = Number.isFinite(Number(options.max_sources)) ? Number(options.max_sources) : 12;
    const lookbackDays = Number.isFinite(Number(options.lookback_days)) ? Number(options.lookback_days) : 14;
    const { response, body } = await fetchJsonWithRetry(
      `${API_BASE}/api/gantt-runs/${encodeURIComponent(runId)}/delta-check`
      + `?force=${encodeURIComponent(force)}`
      + `&max_sources=${encodeURIComponent(maxSources)}`
      + `&lookback_days=${encodeURIComponent(lookbackDays)}`,
      { method: 'POST' },
      { retries: 2, timeoutMs: 35000 }
    );
    if (!response.ok) {
      const detail = errorDetailFromPayload(body);
      throw new Error(detail ? `Failed to run delta-check: ${detail}` : 'Failed to run delta-check');
    }
    return body;
  },

  /**
   * Load a markdown memo artifact from outputs/.
   */
  async getMemo(memoName) {
    const response = await fetch(`${API_BASE}/api/memos/${encodeURIComponent(memoName)}`);
    if (!response.ok) {
      throw new Error('Failed to load memo artifact');
    }
    return response.json();
  },

  /**
   * List all conversations.
   */
  async listConversations() {
    const response = await fetch(`${API_BASE}/api/conversations`);
    if (!response.ok) {
      throw new Error('Failed to list conversations');
    }
    return response.json();
  },

  /**
   * Create a new conversation.
   */
  async createConversation() {
    const response = await fetch(`${API_BASE}/api/conversations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      throw new Error('Failed to create conversation');
    }
    return response.json();
  },

  /**
   * Get a specific conversation.
   */
  async getConversation(conversationId) {
    const response = await fetch(
      `${API_BASE}/api/conversations/${conversationId}`
    );
    if (!response.ok) {
      throw new Error('Failed to get conversation');
    }
    return response.json();
  },

  /**
   * Send a message in a conversation.
   */
  async sendMessage(conversationId, content) {
    const response = await fetch(
      `${API_BASE}/api/conversations/${conversationId}/message`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ content }),
      }
    );
    if (!response.ok) {
      throw new Error('Failed to send message');
    }
    return response.json();
  },

  /**
   * Send a message and receive streaming updates.
   * @param {string} conversationId - The conversation ID
   * @param {string} content - The message content
   * @param {boolean} enableSearch - Whether to enable internet search
   * @param {File[]} files - Array of PDF files to upload
   * @param {File|null} supplementaryFile - Optional supplementary document to enrich analysis
   * @param {string|null} ticker - Optional ticker code (e.g., "ASX:WWI", "NYSE:NEM")
   * @param {string|null} councilMode - Council execution mode ("local" | "perplexity_emulated")
   * @param {string|null} researchDepth - Research depth ("basic" | "deep")
   * @param {string|null} templateId - Optional template ID (e.g., "gold_miner")
   * @param {string|null} companyType - Optional company type (e.g., "gold_miner")
   * @param {string|null} exchange - Optional exchange id (e.g., "asx", "nyse")
   * @param {function} onEvent - Callback function for each event: (eventType, data) => void
   * @returns {Promise<void>}
   */
  async sendMessageStream(
    conversationId,
    content,
    enableSearch,
    files,
    supplementaryFile,
    ticker,
    councilMode,
    researchDepth,
    templateId,
    companyType,
    exchange,
    onEvent
  ) {
    // Create FormData for multipart upload
    const formData = new FormData();
    formData.append('content', content);
    formData.append('enable_search', enableSearch.toString());

    // Add ticker if provided
    if (ticker) {
      formData.append('ticker', ticker);
    }

    // Add council mode if provided
    if (councilMode) {
      formData.append('council_mode', councilMode);
    }

    // Add research depth if provided
    if (researchDepth) {
      formData.append('research_depth', researchDepth);
    }

    // Add template ID if provided
    if (templateId) {
      formData.append('template_id', templateId);
    }

    // Add company type if provided
    if (companyType) {
      formData.append('company_type', companyType);
    }

    // Add exchange if provided
    if (exchange) {
      formData.append('exchange', exchange);
    }

    // Add all files
    if (files && files.length > 0) {
      files.forEach(file => {
        formData.append('files', file);
      });
    }

    if (supplementaryFile) {
      formData.append('supplementary_file', supplementaryFile);
    }

    const response = await fetch(
      `${API_BASE}/api/conversations/${conversationId}/message/stream`,
      {
        method: 'POST',
        // Note: Do NOT set Content-Type header - browser will set it with boundary
        body: formData,
      }
    );

    if (!response.ok) {
      throw new Error('Failed to send message');
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    const processBuffer = () => {
      buffer = buffer.replace(/\r\n/g, '\n');
      let boundary = buffer.indexOf('\n\n');
      while (boundary !== -1) {
        const rawEvent = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);

        const dataLines = rawEvent
          .split('\n')
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trimStart());

        if (dataLines.length > 0) {
          const payload = dataLines.join('\n');
          try {
            const event = JSON.parse(payload);
            onEvent(event.type, event);
          } catch (e) {
            console.error('Failed to parse SSE event:', e);
          }
        }

        boundary = buffer.indexOf('\n\n');
      }
    };

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      processBuffer();
    }

    buffer += decoder.decode();
    processBuffer();

    const trailing = buffer.trim();
    if (trailing.startsWith('data:')) {
      const payload = trailing
        .split('\n')
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).trimStart())
        .join('\n');
      if (payload) {
        try {
          const event = JSON.parse(payload);
          onEvent(event.type, event);
        } catch (e) {
          console.error('Failed to parse trailing SSE event:', e);
        }
      }
    }
  },
};
