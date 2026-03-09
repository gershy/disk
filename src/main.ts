import { isCls } from '@gershy/clearing';
import nodePath from 'node:path';
import { Lore, Fp, LineageLock, Scholar } from './setup.ts';
import fs, { wrapFsError } from './fs.ts';
import retry from '@gershy/util-retry';
import { Readable } from 'node:stream';

export class DiskLore implements Lore {
  
  static defaultDataCmp =  '~';
  static getTmpCmp = () => `~${(Number[int32] * Math.random())[toStr](String[base32], 7)}`;
  
  constructor() {}
  async safeStat(fp: Fp) {
    try              { return await fs.stat(fp.fsp()); }
    catch(err: any) { if (err.code !== 'ENOENT') throw err; }
    return null;
  }
  async getType(fp: Fp) {
    
    const stat = await this.safeStat(fp);
    if (stat === null)      return null;
    if (stat.isFile())      return 'leaf';
    if (stat.isDirectory()) return 'node';
    
    throw Error('unexpected filesystem entity')[mod]({ stat });
    
  }
  async swapLeafToNode(fp: Fp, { tmpCmp=DiskLore.getTmpCmp() }={}) {
    
    // We want a dir to replace an existing file (without reads on that previously existing file to
    // fail) - so we replace the file with a directory containing a "default data file"
    
    // Basically we know that `fp` is a leaf, and we want it to become a node, with
    // `fp.kid(this.constructor.defaultDataCmp)` holding the data previously at `fp`
    
    const fsp = fp.fsp();                              // Path to original file
    const tmpFsp = fp.sib(tmpCmp).fsp();               // Path to temporary file (sibling of original file)
    const valFsp = fp.kid(DiskLore.defaultDataCmp).fsp(); // Path to final file
    
    await fs.rename(fsp, tmpFsp);    // Move file out of the way
    await fs.mkdir(fsp);             // Set directory where file used to be
    await fs.rename(tmpFsp, valFsp); // Set original file as "default data file"
    
  }
  async ensureNode(fp: Fp, { earliestUncertainFp=new Fp([]) }={}) {
    
    // Ensure all ancestor nodes up to (but excluding) `fp`; overall ensures that an entity can be
    // written at `fp`. It doesn't touch `fp`, only `fp`'s ancestors!
    
    let ptr = earliestUncertainFp;
    while (!ptr.equals(fp)) {
      
      const type = await this.getType(ptr);
      
      // If nothing exists create dir; if file exists swap it to dir
      if      (type === null)   await fs.mkdir(ptr.fsp());
      else if (type === 'leaf') await this.swapLeafToNode(ptr);
      
      // Extend `ptr` with the next component in `fp`
      ptr = ptr.kid(fp.cmps[ptr.count()]);
      
    }
    
  }
  async ensureLineageLocks(lineageLocks: LineageLock[]) {
    
    // Note that `ensureNode` isn't being used, since the context always wants to be able to
    // resolve lineage-locks asap, and `ensureNode` doesn't expose a way to do this
    
    if (lineageLocks[empty]()) return;
    
    let lastFp: Fp = lineageLocks[0].fp;
    for (const { fp, prm } of lineageLocks) {
      
      if (!lastFp.contains(fp)) throw Error('Invalid lineage');
      lastFp = fp;
      
      const type = await this.getType(fp);
      if (type === null)        await fs.mkdir(fp.fsp());
      else if (type === 'leaf') await this.swapLeafToNode(fp);
      
      prm.resolve();
      
    }
    
  }
  async remEmptyAncestors(fp: Fp) {
    
    // The passed `fp` should be the first potentially empty *directory* - do not pass a file!
    
    while (true) {
      
      const dir = await fs.readdir(fp.fsp()).catch(err => {
        if (err.code === 'ENOENT') return [] as string[];
        throw err;
      });
      
      // Stop as soon as we encounter a non-empty directory; note an empty "~" file doesn't count!
      if (dir.length === 1 && dir[0] === '~') {
        // If the only child is an empty "~" node, delete it and continue...
        const stat = await this.safeStat(fp.kid('~'))
        if (stat?.size) break;
        await this.remNode(fp.kid('~'));
      } else if (dir.length) {
        break;
      }
      
      // Remove any empty directories
      await retry({
        attempts: 5,
        opts: { delay: n => n * 50 },
        fn: () => fs.rmdir(fp.fsp()).catch(err => {
          if (err.code === 'ENOENT')    return; // Success - nonexistence is the desired state!
          if (err.code === 'ENOTEMPTY') return; // Success - dir is non-empty, so no work to do
          if (err.code === 'EPERM')     throw err[mod]({ retry: true }); // Retry on EPERM
          throw err;
        })
      });
      
      fp = fp.par();
      
    }
    
  }
  async remNode(fp: Fp) {
    
    try              { await fs.unlink(fp.fsp()); }
    catch(err: any) { if (err.code !== 'ENOENT') throw err; }
    
  }
  async setData(lineageLocks: LineageLock[], fp: Fp, data: Buffer | string | Obj<Json> | Json[]) {
    
    const type = await this.getType(fp);
    
    if (isCls(data, Object) || isCls(data, Array)) data = JSON.stringify(data);
    
    if (type === null) {
      
      // Ensure lineage; once this loop is over we know `fp.par()`
      // certainly exists, and `fp` itself doesn't
      await this.ensureLineageLocks(lineageLocks);
      await fs.writeFile(fp.fsp(), data);
      
    } else {
      
      // `fp` is pre-existing! immediately resolve all lineage locks and simply write to either the
      // plain file or "~" kid
      
      // All lineage locks are released; we don't touch the lineage!
      for (const { prm } of lineageLocks) prm.resolve();
      
      const fsp = type === 'node' ? nodePath.join(fp.fsp(), '~') : fp.fsp();
      await fs.writeFile(fsp, data);
      
    }
    
  }
  async getData(fp: Fp, enc: 'bin' | 'str' | 'json' | any) {
    
    const fsp = fp.fsp();
    const type = await this.getType(fp);
    
    const buff = await (async () => {
      
      if (type === null) return Buffer.alloc(0);
      
      const fp = (type === 'leaf') ? fsp : nodePath.join(fsp, '~');
      
      return fs.readFile(fp).catch(err => {
        if (err.code === 'ENOENT') return Buffer.alloc(0);
        throw err;
      });
      
    })();
    
    if (enc === 'bin')  return buff;
    if (enc === 'str')  return buff.toString('utf8');
    if (enc === 'json') try { return buff.length ? JSON.parse(buff as any) : null; } catch (err) { throw Error('non-json'); }
    throw Error('unexpected enc')[mod]({ enc });
    
  }
  async remSubtree(fp: Fp) {
    
    await retry({
      attempts: 5,
      opts: { delay: n => n * 50 },
      fn: n => {
        return fs.rm(fp.fsp(), { recursive: true }).catch(err => {
          if (err.code === 'ENOENT') return;                         // Success - nonexistence is the desired state
          if (err.code === 'EPERM')  throw err[mod]({ retry: true }); // Retry on EPERM
          throw err;
        })
      }
    });
    
    await this.remEmptyAncestors(fp.par());
    
  }
  async getKidCmps(fp: Fp): Promise<string[]> {
    
    return fs.readdir(fp.fsp())
      .then(cmps => cmps.filter(cmp => cmp !== '~'))
      .catch(err => {
        if (err.code === 'ENOENT') return []; // No kids for non-existing entity
        if (err.code === 'ENOTDIR') return []; // The user passed a non-directory
        throw err;
      });
    
  }
  async getHeadStreamAndFinalizePrm(lineageLocks: LineageLock[], fp: Fp) {
    
    await this.ensureLineageLocks(lineageLocks);
    
    const type = await this.getType(fp);
    if (type === 'node') fp = fp.kid('~');
    
    const err = Error('');
    const fsStream = fs.createWriteStream(fp.fsp());
    
    let didWrite = false;
    const stream = {
      
      // TODO: What about backpressure? If we use the synchronous `fsStream.write` method then
      // chunks will still be flushed in order (Writable guarantees this internally), but we are
      // subject to backpressure issues - i.e., `fsStream.write` returns `false`, and we need to
      // wait for a "drain" event before writing more
      
   // write: async (data: string | Buffer) => { if (data.length) didWrite = true; return new Promise<void>((rsv, rjc) => fsStream.write(data, err => err ? rjc(err) : rsv())); },
      write: async (data: string | Buffer) => { if (data.length) didWrite = true; void fsStream.write(data); },
      end:   async ()                      => { fsStream.end(); await prm; }
      
    };
    
    const prm = new Promise<void>((rsv, rjc) => {
      fsStream.on('close', rsv);
      fsStream.on('error', rjc);
    })
      .catch(cause => wrapFsError(err, { cause, name: 'createWriteStream', fsp: fp.fsp() }))
      .finally(async () => {
        
        if (didWrite) return;
        
        const stat = await this.safeStat(fp);
        if (!stat?.isFile()) return;
        if (stat.size > 0)   return;
        
        await this.remNode(fp);
        await this.remEmptyAncestors(fp.par());
        
      });
    
    return { stream, prm };
    
  }
  async getTailStreamAndFinalizePrm(fp: Fp) {
    
    const type = await this.getType(fp);
    
    if (type === null) {
      
      const nullReadable = new Readable();
      (async () => nullReadable.push(null))();
      return { stream: nullReadable, prm: Promise.resolve() };
      
    }
    
    if (type === 'node') return this.getTailStreamAndFinalizePrm(fp.kid('~')); // Recurse into the "~" dir
    
    const err = Error();
    const stream = fs.createReadStream(fp.fsp());
    const prm = new Promise<void>((rsv, rjc) => {
      stream.on('close', rsv);
      stream.on('error', (cause: any) => {
        
        // ENOENT indicates the stream should return no data, successfully
        if (cause.code === 'ENOENT') return rsv();
        
        // ERR_STREAM_PREMATURE_CLOSE unwantedly propagates to the top-level; it should reject
        // like any other error, but need to:
        // 1. Suppress to prevent top-level crash
        // 2. Wrap in a separate error which is then thrown; this ensures the error will crash
        //    at the top-level if it is unhandled
        if (cause.code === 'ERR_STREAM_PREMATURE_CLOSE') {
          cause.suppress();
          cause = err[mod]({ msg: 'broken stream likely caused by unexpected client socket disruption', cause });
        }
        
        return rjc(cause);
        
      });
    });
    
    return { stream, prm };
    
  }
  async getKidIteratorAndFinalizePrm(fp: Fp, { bufferSize = 150 }: { bufferSize?: number } = {}) {
    
    const dir = await fs.opendir(fp.fsp(), { bufferSize }).catch(err => {
      if (err.code === 'ENOENT') return null; // `null` represents no children
      throw err;
    });
    
    if (!dir) return {
      // No yields; immediate completion
      async* [Symbol.asyncIterator](): AsyncGenerator<string> {},
      async close() {},
      prm: Promise.resolve()
    };
    
    const prm = Promise[later]<void>();
    return {
      async* [Symbol.asyncIterator](): AsyncGenerator<string> {
        
        for await (const ent of dir)
          if (ent.name !== '~')
            yield ent.name
        prm.resolve();
        
      },
      async close() {
        
        await new Promise<void>((rsv, rjc) => dir.close(err => err ? rjc(err) : rsv()))
          .catch(err => {
            if (err.code === 'ERR_DIR_CLOSED') return; // Tolerate this error; it indicates multiple attempts to close, which is fine!
            throw err;
          })
          .then(prm.resolve, prm.reject);
        
      },
      prm
    };
    
  }
  
};

export const rootDiskLore = new DiskLore();
export const rootTx = new Scholar(rootDiskLore, []);
export const rootFact = rootTx.getEnt();

export      { Fp, Fact, Scholar } from './setup.ts';
export type { Lore }              from './setup.ts';