/**
 * API client for the LLM Council backend.
 */

const API_BASE = 'http://localhost:8001';

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
