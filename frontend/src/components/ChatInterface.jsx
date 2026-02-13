import { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { api } from '../api';
import Stage1 from './Stage1';
import Stage2 from './Stage2';
import Stage3 from './Stage3';
import './ChatInterface.css';

export default function ChatInterface({
  conversation,
  onSendMessage,
  isLoading,
}) {
  const [input, setInput] = useState('');
  const [ticker, setTicker] = useState('');
  const [enableSearch, setEnableSearch] = useState(true);
  const [selectedFiles, setSelectedFiles] = useState([]);
  const [fileInputKey, setFileInputKey] = useState(Date.now());
  const [availableTemplates, setAvailableTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState('');
  const [availableCompanyTypes, setAvailableCompanyTypes] = useState([]);
  const [selectedCompanyType, setSelectedCompanyType] = useState('');
  const [availableExchanges, setAvailableExchanges] = useState([]);
  const [selectedExchange, setSelectedExchange] = useState('');
  const [councilMode, setCouncilMode] = useState('local');
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [conversation]);

  // Load available templates on mount
  useEffect(() => {
    const loadSelectors = async () => {
      try {
        const [templates, companyTypes, exchanges] = await Promise.all([
          api.listTemplates(),
          api.listCompanyTypes(),
          api.listExchanges(),
        ]);
        setAvailableTemplates(templates || []);
        setAvailableCompanyTypes(companyTypes || []);
        setAvailableExchanges(exchanges || []);
      } catch (error) {
        console.error('Failed to load selector data:', error);
      }
    };
    loadSelectors();
  }, []);

  const handleFileSelect = (e) => {
    const files = Array.from(e.target.files);
    const validFiles = files.filter(f => {
      if (!f.name.endsWith('.pdf')) {
        alert(`${f.name} is not a PDF file`);
        return false;
      }
      if (f.size > 50 * 1024 * 1024) {
        alert(`${f.name} exceeds 50MB limit`);
        return false;
      }
      return true;
    });
    setSelectedFiles(prev => [...prev, ...validFiles]);
  };

  const removeFile = (index) => {
    setSelectedFiles(prev => prev.filter((_, i) => i !== index));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (input.trim() && !isLoading) {
      onSendMessage(
        input,
        enableSearch,
        selectedFiles,
        ticker.trim() || null,
        councilMode || 'local',
        null,
        selectedTemplate || null,
        selectedCompanyType || null,
        selectedExchange || null
      );
      setInput('');
      setTicker('');
      setSelectedFiles([]);
      setSelectedTemplate('');
      setSelectedCompanyType('');
      setSelectedExchange('');
      setFileInputKey(Date.now());
    }
  };

  const handleKeyDown = (e) => {
    // Submit on Enter (without Shift)
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  if (!conversation) {
    return (
      <div className="chat-interface">
        <div className="empty-state">
          <h2>Welcome to LLM Council</h2>
          <p>Create a new conversation to get started</p>
        </div>
      </div>
    );
  }

  return (
    <div className="chat-interface">
      <div className="messages-container">
        {conversation.messages.length === 0 ? (
          <div className="empty-state">
            <h2>Start a conversation</h2>
            <p>Ask a question to consult the LLM Council</p>
          </div>
        ) : (
          conversation.messages.map((msg, index) => (
            <div key={index} className="message-group">
              {msg.role === 'user' ? (
                <div className="user-message">
                  <div className="message-label">You</div>
                  <div className="message-content">
                    <div className="markdown-content">
                      <ReactMarkdown>{msg.content}</ReactMarkdown>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="assistant-message">
                  <div className="message-label">LLM Council</div>
                  {msg.metadata?.council_mode && (
                    <div className="search-query">
                      <strong>Council mode:</strong> {msg.metadata.council_mode}
                      {msg.metadata?.research_depth ? ` | Depth: ${msg.metadata.research_depth}` : ''}
                    </div>
                  )}
                  {(msg.metadata?.template_id || msg.metadata?.company_type || msg.metadata?.exchange) && (
                    <div className="search-query">
                      {[
                        msg.metadata?.template_id ? `Template: ${msg.metadata.template_id}` : null,
                        msg.metadata?.company_name ? `Company: ${msg.metadata.company_name}` : null,
                        msg.metadata?.company_type ? `Company type: ${msg.metadata.company_type}` : null,
                        msg.metadata?.exchange ? `Exchange: ${msg.metadata.exchange}` : null,
                      ].filter(Boolean).join(' | ')}
                    </div>
                  )}

                  {/* Search Results Display */}
                  {msg.loading?.search && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>
                        {msg.search_results?.search_type === 'market_data_only'
                          ? 'Searching for market data (share price, market cap, etc.)...'
                          : 'Searching the internet...'}
                      </span>
                    </div>
                  )}
                  {msg.search_results && msg.search_results.results && msg.search_results.results.length > 0 && (
                    <div className="search-results-section">
                      <h4>
                        {msg.search_results.search_type === 'market_data_only'
                          ? '📊 Market Data Search Results'
                          : 'Search Results Used'}
                      </h4>
                      <div className="search-query">
                        <em>{msg.search_results.query}</em>
                      </div>
                      <div className="search-results-list">
                        {msg.search_results.results.map((result, idx) => (
                          <div key={idx} className="search-result-item">
                            <a href={result.url} target="_blank" rel="noopener noreferrer">
                              {result.title}
                            </a>
                            <p className="result-snippet">{result.content}</p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Evidence Pack Display */}
                  {msg.loading?.evidence && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>Building normalized evidence pack...</span>
                    </div>
                  )}
                  {msg.evidence_pack && (
                    <div className="search-results-section">
                      <h4>Evidence Pack</h4>
                      <div className="search-query">
                        <em>
                          Provider: {msg.evidence_pack.provider || 'unknown'} | Depth: {msg.evidence_pack.depth || 'unknown'}
                        </em>
                      </div>

                      {msg.evidence_pack.key_facts && msg.evidence_pack.key_facts.length > 0 && (
                        <div className="search-results-list">
                          {msg.evidence_pack.key_facts.slice(0, 5).map((fact, idx) => (
                            <div key={idx} className="search-result-item">
                              <p className="result-snippet">{fact}</p>
                            </div>
                          ))}
                        </div>
                      )}

                      {msg.evidence_pack.sources && msg.evidence_pack.sources.length > 0 && (
                        <div className="search-results-list">
                          {msg.evidence_pack.sources.slice(0, 8).map((source, idx) => (
                            <div key={idx} className="search-result-item">
                              <a href={source.url} target="_blank" rel="noopener noreferrer">
                                {source.title || source.url}
                              </a>
                              <p className="result-snippet">
                                {source.source_type ? `[${source.source_type}] ` : ''}
                                {source.snippet || ''}
                              </p>
                            </div>
                          ))}
                        </div>
                      )}

                      {msg.evidence_pack.missing_data && msg.evidence_pack.missing_data.length > 0 && (
                        <div className="search-query">
                          <strong>Missing data:</strong> {msg.evidence_pack.missing_data.join(' | ')}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Attachments Display */}
                  {msg.loading?.attachments && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>Processing PDF attachments...</span>
                    </div>
                  )}
                  {msg.attachments_processed && msg.attachments_processed.length > 0 && (
                    <div className="attachments-section">
                      <h4>Documents Processed</h4>
                      {msg.attachments_processed.map((att, idx) => (
                        <div key={idx} className="attachment-item">
                          <span className="attachment-name">{att.filename}</span>
                          <span className={`attachment-status status-${att.status}`}>
                            {att.status}
                          </span>
                          {att.summary && (
                            <p className="attachment-summary">{att.summary}</p>
                          )}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Stage 1 */}
                  {msg.loading?.stage1 && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>Running Stage 1: Collecting individual responses...</span>
                    </div>
                  )}
                  {msg.stage1 && <Stage1 responses={msg.stage1} />}

                  {/* Stage 2 */}
                  {msg.loading?.stage2 && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>Running Stage 2: Peer rankings...</span>
                    </div>
                  )}
                  {msg.stage2 && (
                    <Stage2
                      rankings={msg.stage2}
                      labelToModel={msg.metadata?.label_to_model}
                      aggregateRankings={msg.metadata?.aggregate_rankings}
                    />
                  )}

                  {/* Stage 3 */}
                  {msg.loading?.stage3 && (
                    <div className="stage-loading">
                      <div className="spinner"></div>
                      <span>Running Stage 3: Final synthesis...</span>
                    </div>
                  )}
                  {msg.stage3 && <Stage3 finalResponse={msg.stage3} />}
                </div>
              )}
            </div>
          ))
        )}

        {isLoading && (
          <div className="loading-indicator">
            <div className="spinner"></div>
            <span>Consulting the council...</span>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <form className="input-form" onSubmit={handleSubmit}>
          {/* Search Toggle */}
          <div className="search-toggle-container">
            <label className="toggle-label">
              <input
                type="checkbox"
                checked={enableSearch}
                onChange={(e) => setEnableSearch(e.target.checked)}
                disabled={isLoading || councilMode === 'perplexity_emulated'}
              />
              <span className="toggle-text">
                {councilMode === 'perplexity_emulated'
                  ? 'Internet search enabled (required for emulated mode)'
                  : 'Enable internet search'}
              </span>
            </label>
          </div>

          {/* Council Mode Selector */}
          <div className="template-selector-container">
            <label htmlFor="council-mode-select" className="template-label">
              Council Mode:
            </label>
            <select
              id="council-mode-select"
              className="template-select"
              value={councilMode}
              onChange={(e) => {
                const nextMode = e.target.value;
                setCouncilMode(nextMode);
                if (nextMode === 'perplexity_emulated') {
                  setEnableSearch(true);
                }
              }}
              disabled={isLoading}
            >
              <option value="local">Local council (current)</option>
              <option value="perplexity_emulated">Perplexity emulated deep-research council</option>
            </select>
            <span className="template-hint">
              Perplexity emulated mode runs one deep-research pass per configured model, then uses Stage 2/3 council judging.
            </span>
          </div>

          {/* Analysis Template Selector */}
          <div className="template-selector-container">
            <label htmlFor="template-select" className="template-label">
              Topic / Analysis Template:
            </label>
            <select
              id="template-select"
              className="template-select"
              value={selectedTemplate}
              onChange={(e) => setSelectedTemplate(e.target.value)}
              disabled={isLoading}
            >
              <option value="">Auto-detect (recommended)</option>
              {availableTemplates.map((template) => (
                <option key={template.id} value={template.id}>
                  {template.name}
                </option>
              ))}
            </select>
            <span className="template-hint">
              Leave on "Auto-detect" to infer a topic from your question (e.g., financial quality score)
            </span>
          </div>

          {/* Company Type Selector */}
          <div className="template-selector-container">
            <label htmlFor="company-type-select" className="template-label">
              Company Type:
            </label>
            <select
              id="company-type-select"
              className="template-select"
              value={selectedCompanyType}
              onChange={(e) => setSelectedCompanyType(e.target.value)}
              disabled={isLoading}
            >
              <option value="">Auto-detect (recommended)</option>
              {availableCompanyTypes.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
            <span className="template-hint">
              Optional manual override for sector rubric routing (e.g., Gold Miner, Pharma/Biotech).
            </span>
          </div>

          {/* Exchange Selector */}
          <div className="template-selector-container">
            <label htmlFor="exchange-select" className="template-label">
              Exchange:
            </label>
            <select
              id="exchange-select"
              className="template-select"
              value={selectedExchange}
              onChange={(e) => setSelectedExchange(e.target.value)}
              disabled={isLoading}
            >
              <option value="">Auto-detect (recommended)</option>
              {availableExchanges.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
            <span className="template-hint">
              Optional manual override for exchange-specific assumptions (ASX, NYSE, NASDAQ, TSX, TSXV, etc.).
            </span>
          </div>

          {/* Ticker Input - Only show when search is enabled */}
          {enableSearch && (
            <div className="ticker-input-container">
              <label htmlFor="ticker-input" className="ticker-label">
                Ticker (optional):
              </label>
              <input
                id="ticker-input"
                type="text"
                className="ticker-input"
                placeholder="e.g., ASX:WWI, NYSE:NEM, TSXV:ABC"
                value={ticker}
                onChange={(e) => setTicker(e.target.value.toUpperCase())}
                maxLength={16}
                disabled={isLoading}
              />
              <span className="ticker-hint">
                Exchange prefix improves auto-detection (e.g., ASX:, NYSE:, NASDAQ:, TSX:, TSXV:). Upload PDFs manually for detailed analysis.
              </span>
            </div>
          )}

          {/* File Upload Section */}
          <div className="file-upload-section">
            <input
              key={fileInputKey}
              type="file"
              id="pdf-upload"
              accept=".pdf"
              multiple
              onChange={handleFileSelect}
              className="file-input"
            />
            <label htmlFor="pdf-upload" className="file-upload-label">
              Attach PDFs (optional)
            </label>

            {selectedFiles.length > 0 && (
              <div className="selected-files">
                {selectedFiles.map((file, index) => (
                  <div key={index} className="file-chip">
                    <span className="file-name">{file.name}</span>
                    <span className="file-size">
                      ({(file.size / 1024).toFixed(1)} KB)
                    </span>
                    <button
                      type="button"
                      className="remove-file"
                      onClick={() => removeFile(index)}
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          <textarea
            className="message-input"
            placeholder="Ask your question... (Shift+Enter for new line, Enter to send)"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={isLoading}
            rows={3}
          />
          <button
            type="submit"
            className="send-button"
            disabled={!input.trim() || isLoading}
          >
            Send
          </button>
      </form>
    </div>
  );
}
