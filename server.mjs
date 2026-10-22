import { createServer } from 'node:http';
import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, truncateSync, unlinkSync, writeFileSync } from 'node:fs';
import { join, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { File, verify } from 'megajs';

const ROOT = fileURLToPath(new URL('.', import.meta.url));
const DOWNLOADS = join(ROOT, 'downloads');
const indexes = new Map();
const jobs = new Map();
mkdirSync(DOWNLOADS, { recursive: true });

function safeName(name) {
  return basename(String(name || 'mega-download').replace(/[<>:"/\\|?*\x00-\x1f]/g, '_')).slice(0, 180) || 'mega-download';
}

function outputPath(name) {
  const filename = safeName(name);
  const dot = filename.lastIndexOf('.');
  const extension = dot > 0 ? filename.slice(dot) : '';
  const stem = extension ? filename.slice(0, -extension.length) : filename;
  let path = join(DOWNLOADS, filename);
  let suffix = 2;
  while (existsSync(path)) path = join(DOWNLOADS, `${stem} (${suffix++})${extension}`);
  return path;
}

function send(res, code, data, type = 'application/json; charset=utf-8') {
  res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
  res.end(type.startsWith('application/json') ? JSON.stringify(data) : data);
}

async function body(req) {
  let raw = '';
  for await (const part of req) {
    raw += part;
    if (raw.length > 12_000) throw new Error('请求内容过大');
  }
  return JSON.parse(raw || '{}');
}

async function collect(node, out = []) {
  if (node.directory) {
    for (const child of node.children || []) await collect(child, out);
  } else {
    out.push({ id: randomUUID(), name: node.name, size: Number(node.size), node });
  }
  return out;
}

async function verifyFile(path, key) {
  try {
    await pipeline(createReadStream(path), verify(key), new Writable({ write(_chunk, _encoding, callback) { callback(); } }));
  } catch {
    unlinkSync(path);
    throw new Error('完整性校验失败，损坏的临时文件已清理，请重新下载');
  }
}

function runDownload(job, item, connections) {
  job.state = 'downloading';
  job.error = '';
  job.promise = (async () => {
    const key = createHash('sha256').update(String(item.node.downloadId)).update(item.node.key).digest('hex');
    const part = join(DOWNLOADS, `${key}.part`);
    const metadata = join(DOWNLOADS, `${key}.json`);
    job.partId = key;
    writeFileSync(metadata, JSON.stringify({ name: safeName(item.name), total: item.size }));
    let start = existsSync(part) ? statSync(part).size : 0;
    if (start > item.size) start = 0;
    start = Math.floor(start / 16) * 16;
    if (existsSync(part) && statSync(part).size !== start) truncateSync(part, start);
    job.received = start;
    job.total = item.size;
    if (start === item.size && existsSync(part)) {
      try {
        await verifyFile(part, item.node.key);
        job.state = 'complete';
        job.path = outputPath(item.name);
        renameSync(part, job.path);
        unlinkSync(metadata);
        return;
      } catch (err) {
        job.state = 'interrupted';
        job.error = err.message;
        return;
      }
    }
    try {
      const stream = item.node.download({ start, maxConnections: connections });
      job.stream = stream;
      stream.on('data', chunk => { job.received += chunk.length; });
      await pipeline(stream, createWriteStream(part, { flags: start ? 'a' : 'w' }));
      const actual = statSync(part).size;
      if (actual !== item.size) throw new Error(`文件长度不完整（${actual} / ${item.size} 字节），可再次点击续传`);
      await verifyFile(part, item.node.key);
      job.path = outputPath(item.name);
      renameSync(part, job.path);
      unlinkSync(metadata);
      job.state = 'complete';
    } catch (err) {
      job.state = 'interrupted';
      job.error = existsSync(part) ? `${err.message || '下载连接中断'}；已保留临时文件，再点一次可续传` : (err.message || '临时文件已清理，请重新下载');
    } finally {
      job.stream = null;
    }
  })().catch(err => {
    job.state = 'interrupted';
    job.error = err.message || '下载任务启动失败';
    console.error('MEGA download job failed:', err);
  });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  if (req.method === 'GET' && url.pathname === '/') {
    const html = await (await import('node:fs/promises')).readFile(join(ROOT, 'public', 'index.html'), 'utf8');
    return send(res, 200, html, 'text/html; charset=utf-8');
  }
  if (req.method === 'GET' && url.pathname === '/api/partials') {
    const partials = readdirSync(DOWNLOADS).filter(name => /^[a-f0-9]{64}\.part$/.test(name)).map(name => {
      const id = name.slice(0, -5);
      const part = join(DOWNLOADS, name);
      let metadata = {};
      try { metadata = JSON.parse(readFileSync(join(DOWNLOADS, `${id}.json`), 'utf8')); } catch {}
      return { id, name: metadata.name || '旧版临时文件（尚未识别名称）', size: statSync(part).size, total: Number(metadata.total) || 0, updatedAt: statSync(part).mtimeMs };
    });
    return send(res, 200, { partials });
  }
  if (req.method === 'GET' && url.pathname.startsWith('/api/jobs/')) {
    const job = jobs.get(url.pathname.split('/').pop());
    return job ? send(res, 200, { state: job.state, received: job.received, total: job.total, error: job.error, path: job.path }) : send(res, 404, { error: '找不到任务' });
  }
  if (req.method !== 'POST' || !url.pathname.startsWith('/api/')) return send(res, 404, { error: 'Not found' });
  const origin = req.headers.origin;
  if (origin && origin !== 'http://127.0.0.1:8787' && origin !== 'http://localhost:8787') return send(res, 403, { error: '只允许本机页面访问' });
  try {
    const data = await body(req);
    if (url.pathname === '/api/index') {
      let parsed;
      try { parsed = new URL(data.link); } catch { return send(res, 400, { error: '请输入有效的 MEGA 分享链接' }); }
      if (!['mega.nz', 'www.mega.nz', 'mega.co.nz'].includes(parsed.hostname)) return send(res, 400, { error: '只支持 mega.nz 分享链接' });
      const root = File.fromURL(data.link);
      await root.loadAttributes();
      const files = await collect(root);
      if (!files.length) return send(res, 404, { error: '链接中没有可下载文件' });
      const id = randomUUID();
      indexes.set(id, files);
      return send(res, 200, { indexId: id, files: files.map(({ id, name, size }) => ({ id, name, size })) });
    }
    if (url.pathname === '/api/download') {
      const files = indexes.get(data.indexId);
      const item = files?.find(file => file.id === data.fileId);
      if (!item) return send(res, 404, { error: '文件索引已失效，请重新载入分享链接' });
      const connections = [2, 4, 8].includes(Number(data.connections)) ? Number(data.connections) : 2;
      let job = [...jobs.values()].find(j => j.indexId === data.indexId && j.fileId === data.fileId && (j.state === 'downloading' || j.state === 'complete' && existsSync(j.path)));
      if (!job) {
        job = { id: randomUUID(), indexId: data.indexId, fileId: data.fileId, state: 'queued', received: 0, total: item.size, error: '', path: '' };
        jobs.set(job.id, job);
        runDownload(job, item, connections);
      }
      return send(res, 200, { jobId: job.id });
    }
    if (url.pathname === '/api/pause') {
      const job = jobs.get(data.jobId);
      if (!job) return send(res, 404, { error: '找不到下载任务' });
      if (!job.stream || !['downloading', 'paused'].includes(job.state)) return send(res, 409, { error: '当前下载任务不能暂停或继续' });
      if (data.action === 'resume' && job.state === 'paused') {
        job.state = 'downloading';
        job.stream.resume();
      } else if (data.action === 'pause' && job.state === 'downloading') {
        job.state = 'paused';
        job.stream.pause();
      } else {
        return send(res, 409, { error: '下载状态已变化，请稍后重试' });
      }
      return send(res, 200, { state: job.state });
    }
    if (url.pathname === '/api/delete-partial') {
      if (!/^[a-f0-9]{64}$/.test(data.id || '')) return send(res, 400, { error: '无效的临时文件记录' });
      const active = [...jobs.values()].some(job => job.partId === data.id && ['queued', 'downloading', 'paused'].includes(job.state));
      if (active) return send(res, 409, { error: '该文件仍在下载或暂停中，请先结束任务' });
      const part = join(DOWNLOADS, `${data.id}.part`);
      if (!existsSync(part)) return send(res, 404, { error: '临时文件已不存在' });
      unlinkSync(part);
      const metadata = join(DOWNLOADS, `${data.id}.json`);
      if (existsSync(metadata)) unlinkSync(metadata);
      return send(res, 200, { ok: true });
    }
    return send(res, 404, { error: 'Not found' });
  } catch (err) {
    return send(res, 500, { error: err.message || '请求失败' });
  }
});

server.listen(8787, '127.0.0.1', () => console.log('打开 http://127.0.0.1:8787'));
