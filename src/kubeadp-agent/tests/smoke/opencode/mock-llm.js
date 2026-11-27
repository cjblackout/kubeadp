// Minimal OpenAI-compatible mock LLM (no model): canned reply, streaming and non-streaming.
const http = require('http'), fs = require('fs');
const REPLY = 'MOCK-OK from the mock llm';
const log = (s) => { const l = new Date().toISOString() + ' ' + s + '\n'; process.stdout.write(l); try { fs.appendFileSync('/work/mock.log', l); } catch (e) {} };
http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => body += c);
  req.on('end', () => {
    log(req.method + ' ' + req.url + ' bytes=' + body.length);
    if (req.url.startsWith('/v1/models')) { res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify({ object: 'list', data: [{ id: 'mock-1', object: 'model' }] })); }
    if (req.url.startsWith('/v1/chat/completions')) {
      let j = {}; try { j = JSON.parse(body); } catch (e) {}
      const id = 'chatcmpl-mock', created = Math.floor(Date.now() / 1000), model = j.model || 'mock-1';
      if (j.stream) {
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        const chunk = (delta, fin, usage) => 'data: ' + JSON.stringify({ id, object: 'chat.completion.chunk', created, model, choices: [{ index: 0, delta, finish_reason: fin }], ...(usage ? { usage } : {}) }) + '\n\n';
        res.write(chunk({ role: 'assistant', content: '' }, null));
        for (const w of REPLY.split(' ')) res.write(chunk({ content: w + ' ' }, null));
        res.write(chunk({}, 'stop', { prompt_tokens: 7, completion_tokens: 6, total_tokens: 13 }));
        res.write('data: [DONE]\n\n');
        return res.end();
      }
      res.setHeader('content-type', 'application/json');
      return res.end(JSON.stringify({ id, object: 'chat.completion', created, model, choices: [{ index: 0, message: { role: 'assistant', content: REPLY }, finish_reason: 'stop' }], usage: { prompt_tokens: 7, completion_tokens: 6, total_tokens: 13 } }));
    }
    res.statusCode = 404; res.end('{}');
  });
}).listen(8080, '127.0.0.1', () => log('mock llm listening on 127.0.0.1:8080'));
