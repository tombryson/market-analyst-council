import { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { api } from '../api';
import Stage1 from './Stage1';
import Stage2 from './Stage2';
import Stage3 from './Stage3';
import './ChatInterface.css';

function LoadingMeter({ label, progress = null, detail = '', className = '' }) {
  const numericProgress = Number(progress);
  const hasProgress = Number.isFinite(numericProgress);
  const pct = hasProgress ? Math.max(0, Math.min(100, numericProgress)) : null;
  const displayPct = hasProgress ? `${Math.round(pct)}%` : 'Working';

  return (
    <div className={`loading-meter ${className}`.trim()}>
      <div className="loading-meter-topline">
        <span className="loading-meter-label">{label}</span>
        <span className={`loading-meter-pct ${hasProgress ? '' : 'is-muted'}`.trim()}>
          {displayPct}
        </span>
      </div>
      <div className={`loading-meter-track ${hasProgress ? '' : 'is-indeterminate'}`.trim()}>
        <div
          className="loading-meter-fill"
          style={hasProgress ? { width: `${pct}%` } : undefined}
        />
      </div>
      {detail ? <div className="loading-meter-detail">{detail}</div> : null}
    </div>
  );
}

export default function ChatInterface({
  conversation,
  onSendMessage,
  isLoading,
}) {
  const [input, setInput] = useState('');
  const [ticker, setTicker] = useState('');
  const [enableSearch, setEnableSearch] = useState(true);
  const [selectedFiles, setSelectedFiles] = useState([]);
  const [supplementaryFile, setSupplementaryFile] = useState(null);
  const [fileInputKey, setFileInputKey] = useState(Date.now());
  const [supplementaryInputKey, setSupplementaryInputKey] = useState(Date.now() + 1);
  const [availableTemplates, setAvailableTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState('');
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
        const [templates, exchanges] = await Promise.all([
          api.listTemplates(),
          api.listExchanges(),
        ]);
        setAvailableTemplates(templates || []);
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

  const handleSupplementarySelect = (e) => {
    const file = e.target.files?.[0] || null;
    if (!file) return;
    const lowerName = file.name.toLowerCase();
    const allowed = ['.pdf', '.md', '.txt', '.json'];
    if (!allowed.some(ext => lowerName.endsWith(ext))) {
      alert(`${file.name} must be a PDF, Markdown, text, or JSON file`);
      return;
    }
    if (file.size > 20 * 1024 * 1024) {
      alert(`${file.name} exceeds 20MB limit`);
      return;
    }
    setSupplementaryFile(file);
  };

  const removeSupplementaryFile = () => {
    setSupplementaryFile(null);
    setSupplementaryInputKey(Date.now());
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (input.trim() && !isLoading) {
      onSendMessage(
        input,
        enableSearch,
        selectedFiles,
        supplementaryFile,
        ticker.trim() || null,
        councilMode || 'local',
        null,
        selectedTemplate || null,
        null,
        selectedExchange || null
      );
      setInput('');
      setTicker('');
      setSelectedFiles([]);
      setSupplementaryFile(null);
      setSelectedTemplate('');
      setSelectedExchange('');
      setFileInputKey(Date.now());
      setSupplementaryInputKey(Date.now() + 1);
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
                  {(msg.metadata?.company_name || msg.metadata?.exchange) && (
                    <div className="search-query">
                      {[
                        msg.metadata?.company_name ? `Company: ${msg.metadata.company_name}` : null,
                        msg.metadata?.exchange ? `Exchange: ${msg.metadata.exchange}` : null,
                      ].filter(Boolean).join(' | ')}
                    </div>
                  )}

                  {/* Search Results Display */}
                  {msg.loading?.search && (
                    <LoadingMeter
                      label={
                        msg.search_results?.search_type === 'market_data_only'
                          ? 'Searching market data'
                          : 'Searching the internet'
                      }
                      detail={
                        msg.search_results?.search_type === 'market_data_only'
                          ? 'Share price, market cap, and structure facts'
                          : 'Gathering supporting sources'
                      }
                    />
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
                    <LoadingMeter
                      label="Building evidence pack"
                      detail="Normalizing claims and source rows"
                    />
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
                    <LoadingMeter
                      label="Processing PDF attachments"
                      detail="Extracting and normalizing documents"
                    />
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
                    <LoadingMeter
                      label="Running Stage 1"
                      progress={msg.loading?.stage1Progress}
                      detail={
                        msg.loading?.stage1Message ||
                        (msg.loading?.stage1Total
                          ? `${msg.loading.stage1Completed}/${msg.loading.stage1Total} models complete`
                          : 'Collecting individual responses')
                      }
                    />
                  )}
                  {msg.stage1 && <Stage1 responses={msg.stage1} />}

                  {/* Stage 2 */}
                  {msg.loading?.stage2 && (
                    <LoadingMeter
                      label="Running Stage 2"
                      detail="Collecting peer rankings"
                    />
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
                    <LoadingMeter
                      label="Running Stage 3"
                      detail="Final council synthesis"
                    />
                  )}
                  {msg.stage3 && <Stage3 finalResponse={msg.stage3} />}
                </div>
              )}
            </div>
          ))
        )}

        {isLoading && (
          <div className="loading-indicator">
            <LoadingMeter
              label="Consulting the council"
              detail="Coordinating stage-by-stage analysis"
            />
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

          <div className="file-upload-section">
            <input
              key={supplementaryInputKey}
              type="file"
              id="supplementary-upload"
              accept=".pdf,.md,.txt,.json"
              onChange={handleSupplementarySelect}
              className="file-input"
            />
            <label htmlFor="supplementary-upload" className="file-upload-label">
              Attach Supplementary Document (optional)
            </label>
            <span className="template-hint">
              Supplementary context only. Analysis runs normally without it.
            </span>

            {supplementaryFile && (
              <div className="selected-files">
                <div className="file-chip">
                  <span className="file-name">{supplementaryFile.name}</span>
                  <span className="file-size">
                    ({(supplementaryFile.size / 1024).toFixed(1)} KB)
                  </span>
                  <button
                    type="button"
                    className="remove-file"
                    onClick={removeSupplementaryFile}
                  >
                    ×
                  </button>
                </div>
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
