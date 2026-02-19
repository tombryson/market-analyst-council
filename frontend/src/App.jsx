import { useState, useEffect } from 'react';
import Sidebar from './components/Sidebar';
import ChatInterface from './components/ChatInterface';
import GanttMappingDemo from './components/GanttMappingDemo';
import { api } from './api';
import './App.css';

function CouncilApp() {
  const [conversations, setConversations] = useState([]);
  const [currentConversationId, setCurrentConversationId] = useState(null);
  const [currentConversation, setCurrentConversation] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  // Load conversations on mount
  useEffect(() => {
    loadConversations();
  }, []);

  // Load conversation details when selected
  useEffect(() => {
    if (currentConversationId) {
      loadConversation(currentConversationId);
    }
  }, [currentConversationId]);

  const loadConversations = async () => {
    try {
      const convs = await api.listConversations();
      setConversations(convs);
    } catch (error) {
      console.error('Failed to load conversations:', error);
    }
  };

  const loadConversation = async (id) => {
    try {
      const conv = await api.getConversation(id);
      setCurrentConversation(conv);
    } catch (error) {
      console.error('Failed to load conversation:', error);
    }
  };

  const handleNewConversation = async () => {
    try {
      const newConv = await api.createConversation();
      setConversations([
        { id: newConv.id, created_at: newConv.created_at, message_count: 0 },
        ...conversations,
      ]);
      setCurrentConversationId(newConv.id);
    } catch (error) {
      console.error('Failed to create conversation:', error);
    }
  };

  const handleSelectConversation = (id) => {
    setCurrentConversationId(id);
  };

  const handleSendMessage = async (
    content,
    enableSearch,
    files,
    ticker,
    councilMode,
    researchDepth,
    templateId,
    companyType,
    exchange
  ) => {
    if (!currentConversationId) return;

    setIsLoading(true);
    try {
      // Optimistically add user message to UI with metadata
      const userMessage = {
        role: 'user',
        content,
        enable_search: enableSearch,
        ticker: ticker,
        council_mode: councilMode,
        template_id: templateId,
        company_type: companyType,
        exchange: exchange,
        attachments: files.map(f => ({
          filename: f.name,
          size: f.size,
          processing_status: 'pending'
        }))
      };
      setCurrentConversation((prev) => ({
        ...prev,
        messages: [...prev.messages, userMessage],
      }));

      // Create a partial assistant message that will be updated progressively
      const assistantMessage = {
        role: 'assistant',
        stage1: null,
        stage2: null,
        stage3: null,
        search_results: null,
        evidence_pack: null,
        attachments_processed: null,
        metadata: null,
        loading: {
          search: false,
          evidence: false,
          attachments: false,
          stage1: false,
          stage2: false,
          stage3: false,
        },
      };

      // Add the partial assistant message
      setCurrentConversation((prev) => ({
        ...prev,
        messages: [...prev.messages, assistantMessage],
      }));

      // Send message with streaming
      await api.sendMessageStream(
        currentConversationId,
        content,
        enableSearch,
        files,
        ticker,
        councilMode,
        researchDepth,
        templateId,
        companyType,
        exchange,
        (eventType, event) => {
        switch (eventType) {
          case 'council_mode':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.metadata = {
                ...(lastMsg.metadata || {}),
                council_mode: event.data?.mode || 'local',
                research_depth: event.data?.research_depth || 'basic',
              };
              return { ...prev, messages };
            });
            break;

          case 'template_selected':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.metadata = {
                ...(lastMsg.metadata || {}),
                template_id: event.data?.template_id,
                template_name: event.data?.template_name,
                company_name: event.data?.company_name,
                company_type: event.data?.company_type,
                template_selection_source: event.data?.selection_source,
                exchange: event.data?.exchange,
                exchange_selection_source: event.data?.exchange_selection_source,
              };
              return { ...prev, messages };
            });
            break;

          case 'search_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.search = true;
              return { ...prev, messages };
            });
            break;

          case 'search_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.search_results = event.data;
              lastMsg.loading.search = false;
              if (event.data?.evidence_pack && !lastMsg.evidence_pack) {
                lastMsg.evidence_pack = event.data.evidence_pack;
              }
              return { ...prev, messages };
            });
            break;

          case 'evidence_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.evidence = true;
              return { ...prev, messages };
            });
            break;

          case 'evidence_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.evidence_pack = event.data;
              lastMsg.loading.evidence = false;
              return { ...prev, messages };
            });
            break;

          case 'attachments_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.attachments = true;
              return { ...prev, messages };
            });
            break;

          case 'attachments_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.attachments_processed = event.data;
              lastMsg.loading.attachments = false;
              return { ...prev, messages };
            });
            break;

          case 'stage1_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.stage1 = true;
              return { ...prev, messages };
            });
            break;

          case 'stage1_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.stage1 = event.data;
              lastMsg.loading.stage1 = false;
              return { ...prev, messages };
            });
            break;

          case 'stage2_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.stage2 = true;
              return { ...prev, messages };
            });
            break;

          case 'stage2_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.stage2 = event.data;
              lastMsg.metadata = {
                ...(lastMsg.metadata || {}),
                ...(event.metadata || {}),
              };
              lastMsg.loading.stage2 = false;
              return { ...prev, messages };
            });
            break;

          case 'stage3_start':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.loading.stage3 = true;
              return { ...prev, messages };
            });
            break;

          case 'stage3_complete':
            setCurrentConversation((prev) => {
              const messages = [...prev.messages];
              const lastMsg = messages[messages.length - 1];
              lastMsg.stage3 = event.data;
              lastMsg.loading.stage3 = false;
              return { ...prev, messages };
            });
            break;

          case 'title_complete':
            // Reload conversations to get updated title
            loadConversations();
            break;

          case 'complete':
            // Stream complete, reload conversations list
            loadConversations();
            setIsLoading(false);
            break;

          case 'error':
            console.error('Stream error:', event.message);
            setIsLoading(false);
            break;

          default:
            console.log('Unknown event type:', eventType);
        }
        }
      );
    } catch (error) {
      console.error('Failed to send message:', error);
      // Remove optimistic messages on error
      setCurrentConversation((prev) => ({
        ...prev,
        messages: prev.messages.slice(0, -2),
      }));
      setIsLoading(false);
    }
  };

  return (
    <div className="app">
      <Sidebar
        conversations={conversations}
        currentConversationId={currentConversationId}
        onSelectConversation={handleSelectConversation}
        onNewConversation={handleNewConversation}
      />
      <ChatInterface
        conversation={currentConversation}
        onSendMessage={handleSendMessage}
        isLoading={isLoading}
      />
    </div>
  );
}

function App() {
  const path =
    typeof window !== 'undefined'
      ? window.location.pathname.replace(/\/+$/, '')
      : '';
  if (path === '/gantt-demo') {
    return <GanttMappingDemo />;
  }
  return <CouncilApp />;
}

export default App;
