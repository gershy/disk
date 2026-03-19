import nodePath, { PlatformPath } from 'node:path';
import { Stats } from 'node:fs';
import { Readable } from 'node:stream';
import '@gershy/clearing';

const { isCls, map, find, mod, skip, toArr, getClsName, has, later, rem, toObj } = clearing;

export type Lock = { type: string, fp: Fp, prm: PromiseLater };
export type LineageLock = { fp: Fp, prm: PromiseLater };
export type PartialIterator<T> = {
  [Symbol.asyncIterator](): AsyncGenerator<T, any, any>,
  close():                  Promise<void>,
  prm:                      Promise<void>
};

export class Fp {
  
  // alphanum!@ followed by the same including ".", "-" (careful with "-" in regexes), "_", and " "
  static validComponentRegex = /^[a-zA-Z0-9!@][-a-zA-Z0-9!@._ ]*$/;
  
  // All components must have a char that isn't "." (we allow "~" at this level - FilesysTransaction manages this char)
  static illegalComponentRegex = /^[.]+$/;
  
  private path: PlatformPath;
  public cmps: string[];
  public fspVal: null | string;
  constructor(vals: string[], path=nodePath) {
    
    if (!isCls(vals, Array)) vals = [ vals ];
    
    // TODO: Or maybe we should entirely prevent component separators from being passed?
    // Con: it could create directory traversal issues (although only into children, not parents)
    // Pro: it makes it easy to work with e.g. `import.meta.dirname`
    vals = vals
      [map](cmp => cmp.split(/[/\\]+/)) // Each String is broken into its components
      .flat(1);                         // Finally flatten into flat list of components
    
    const illegalCmp = vals[find](val => Fp.illegalComponentRegex.test(val)).val;
    if (illegalCmp) throw Error('illegal file component provided')[mod]({ cmps: vals, illegalCmp });
    
    // Use `path.resolve`; first component being "/" ensures working directory is always ignored;
    // split final result by "/" and "\" which may produce an empty leading item on posix as, e.g.,
    // `'/a/b/c'.split('/') === [ '', 'a', 'b', 'c' ]`
    
    this.path = path;
    this.cmps = path.resolve('/', ...vals).split(/[/\\]+/)[map](v => v || skip);
    this.fspVal = null;
    
  }
  
  toString() { return this.cmps.length ? `file://${this.cmps.join('/')}` : `file://${this.path.resolve('/').split(/[/\\]/).filter(Boolean).join('/')}`; }
  count() { return this.cmps.length; }
  kid(fp: string | string[]) { return new Fp([ this.cmps, fp ].flat(1)); }
  sib(cmp: string) { return new Fp([ this.cmps.slice(0, -1), cmp ].flat(1)); }
  par(n=1) { return (n <= 0) ? this : new Fp(this.cmps.slice(0, -n)); }
  contains(fp) { return this.cmps.length  <= fp.cmps.length && this.cmps.every((v, i) => fp.cmps[i] === v); }
  equals(fp)   { return this.cmps.length === fp.cmps.length && this.cmps.every((v, i) => fp.cmps[i] === v); }
  fsp() { // "file system pointer"
    
    if (!this.fspVal) {
      let fspVal = this.path.resolve('/', ...this.cmps);
      if (/^[A-Z][:]$/.test(fspVal)) fspVal += '\\';
      /// {ASSERT=
      if (!/^([A-Z]+[:])?[/\\]/.test(fspVal)) throw Error('path doesn\'t start with optional drive indicator (e.g. "C:") followed by "/" or "\\"')[mod]({ fp: this, fsp: fspVal });
      /// =ASSERT}
      this.fspVal = fspVal;
    }
    return this.fspVal;
    
  }
  relativeCmps(trg: Fp) {
    
    // Returns an array of components which, after traversing, result in `fp`; note ".." is used to
    // indicate parent nodes!
    
    const [ srcCmps, trgCmps ] = [ this.cmps, trg.cmps ];
    
    const minLen = Math.min(srcCmps.length, trgCmps.length);
    
    let numCommon = 0;
    while (numCommon < minLen && srcCmps[numCommon] === trgCmps[numCommon]) numCommon++;
    
    const numPars = srcCmps.length - numCommon; // Traverse back to the common node
    const remaining = trgCmps.slice(numCommon); // Traverse forward to the target
    
    return [ ...(numPars)[toArr](() => '..'), ...remaining ];
    
  }
  * getLineage(fp: Fp) {
    
    // Yield every Filepath from `this` up to (excluding) `fp`
    if (!this.contains(fp)) throw Error('provided Filepath isn\'t a child');
    
    let ptr: Fp = this;
    while (!ptr.equals(fp)) {
      yield ptr;
      ptr = ptr.kid(fp.cmps[ptr.count()]);
    }
    
  }
  
};

type Writable = {
  write: (data: string | Buffer) => Promise<void>,
  end:   ()                      => Promise<void>
};
export interface Lore {
  
  safeStat: (fp: Fp) => Promise<null | Stats>, // TODO: AbstractSys shouldn't think of this in terms of "stat" - it should just return metadata like byte-size and entity type
  getType: (fp: Fp) => Promise<null | 'leaf' | 'node'>,
  swapLeafToNode: (fp: Fp, opts?: { tmpCmp?: string }) => Promise<void>,
  ensureNode: (fp: Fp, opts?: { earliestUncertainFp?: Fp }) => Promise<void>,
  ensureLineageLocks: (lineageLocks: LineageLock[]) => Promise<void>,
  
  setData: {
    // Note no encoding is specified when setting data - the data's type determines the encoding
    (lineageLocks: LineageLock[], fp: Fp, data: Buffer | string | Obj<Json> | Json[]): Promise<void>,
  },
  getData: {
    // Note the user must specify an encoding when getting data
    // Note that if the user specifies "str" encoding but the underlying data is not utf8, it will
    // be returned with 0xfffd ("replacement character") substituted in all non-utf8 locations
    (fp: Fp, opts: 'bin'):  Promise<Buffer>,
    (fp: Fp, opts: 'str'):  Promise<string>,
    (fp: Fp, opts: 'json'): Promise<null | Obj<Json> | Json[]>
    (fp: Fp, opts: 'bin' | 'str' | 'json'): Promise<Buffer | string | null | Obj<Json> | Json[]>
  },
  
  remSubtree: (fp: Fp) => Promise<void>,
  getKidCmps: (fp: Fp) => Promise<string[]>,
  remEmptyAncestors: (fp: Fp) => Promise<void>,
  remNode: (fp: Fp) => Promise<void>,
  getHeadStreamAndFinalizePrm: (lineageLocks: LineageLock[], fp: Fp) => Promise<{ stream: Writable, prm: Promise<void> }>,
  getTailStreamAndFinalizePrm: (fp: Fp) => Promise<{ stream: Readable, prm: Promise<void> }>,
  getKidIteratorAndFinalizePrm: (fp: Fp, opts?: { bufferSize: number }) => Promise<PartialIterator<string>>
  
};

export class Scholar<L extends Lore> {
  
  public fp: Fp;
  private lore: Lore;
  private locks: Set<Lock>;
  private active: boolean;
  private endFns: Array<(...args: any[]) => any>;
  
  constructor(lore: L, fp: string[] | Fp = []) {
    
    this.fp = isCls(fp, Array) ? new Fp(fp) : fp;
    this.lore = lore;
    this.locks = new Set();
    this.active = true;
    this.endFns = [];
    
  }
  
  toString() { return `${getClsName(this)} @ ${this.fp.toString()}`; }
  
  getEnt() {
    return new Fact(this, this.fp);
  }
  checkFp(fp: Fp) {
    if (!this.fp.contains(fp))            throw Error('fp is not contained within the transaction')[mod]({ fp, tx: this });
    if (fp.cmps.some(cmp => cmp === '~')) throw Error('fp must not contain "~" component')[mod]({ fp });
  }
  locksCollide(lock0, lock1) {
    
    // Order `lock0` and `lock1` by their "type" properties
    if (lock0.type.localeCompare(lock1.type) > 0) [ lock0, lock1 ] = [ lock1, lock0 ];
    
    const collTypeKey = `${lock0.type}/${lock1.type}`;
    
    if (collTypeKey === 'nodeRead/nodeRead') return false; // Reads never collide with each other!
    
    if (collTypeKey === 'nodeRead/nodeWrite') {
      
      // Reads and writes only conflict if they occur on the exact same node
      return lock0.fp.equals(lock1.fp);
      
    }
    
    if (collTypeKey === 'nodeRead/subtreeWrite') {
      
      // Conflict if the node being read is within the subtree
      return lock1.fp.contains(lock0.fp);
      
    }
    
    if (collTypeKey === 'nodeWrite/nodeWrite') {
      
      // Writes aren't allowed to race with each other - two writes
      // collide if they occur on the exact same node!
      return lock0.fp.equals(lock1.fp);
      
    }
    
    if (collTypeKey === 'nodeWrite/subtreeWrite') {
      
      // Conflict if the node being written is within the subtree
      return lock1.fp.contains(lock0.fp);
      
    }
    
    if (collTypeKey === 'subtreeWrite/subtreeWrite') {
      
      // Conflict if either node contains the other; at first this intuitively feels like subtree
      // writes will almost always lock each other out, but this intuition is misleading! Tree-like
      // structures have "sufficient width" in such a way that, given two arbitrary nodes in any
      // large tree, it's unlikely either node contains the other. Consider two nodes "miss" each
      // other when their common ancestor is distinct from either of them (common!)
      return lock0.fp.contains(lock1.fp) || lock1.fp.contains(lock0.fp);
      
    }
    
    throw Error(`collision type "${collTypeKey}" not implemented`);
    
  }
  async doLocked<Fn extends () => any>({ name='?', locks=[], fn, err }: { name: string, locks: any[], fn: Fn, err?: any }): Promise<Awaited<ReturnType<Fn>>> {
    
    if (!this.active) throw Error('inactive transaction');
    
    for (const lock of locks) if (!lock[has]('prm')) lock.prm = Promise[later]();
    
    // Collect all pre-existing locks that collide with any of the locks
    // provided for this operation (once all collected Promises have
    // resolved we will be guaranteed we have a safely locked context!)
    const collLocks: any[] = [];
    for (const lk0 of this.locks) for (const lk1 of locks) if (this.locksCollide(lk0, lk1)) { collLocks.push(lk0); break; }
    
    // We've got our "prereq" Promise - now add a new Lock so any new
    // actions are blocked until `fn` completes
    for (const lock of locks) { this.locks.add(lock); lock.prm.then(() => this.locks[rem](lock)); }
    
    // Initialize the stack Error before any `await` gets called
    if (!err) err = Error('');
    
    // Wait for all collisions to resolve...
    await Promise.all(collLocks[map](lock => lock.prm)); // Won't reject because it's a Promise.all over Locks, and no `Lock(...).prm` ever rejects!
    
    // We now own the locked context!
    try          { return await fn(); }
    catch(cause) { throw err[mod]({ cause, msg: `Failed locked op: "${name}"` }); }
    finally      { for (const lock of locks) lock.prm.resolve(); } // Force any remaining Locks to resolve
    
  }
  async transact<T>({ name='?', fp, fn }: { name: string, fp: Fp, fn: (tx: Scholar<L>) => Promise<T> }) {
    
    // Maybe functions can pass in a whole bunch of initial locks with various bounding; the caller
    // can end these locks whenever they see fit (and `doLocked` can simply remove entries from
    // `this.locks` when the corresponding task resolves - not just at the end of the function!!)
    
    this.checkFp(fp);
    
    const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }) as LineageLock);
    return this.doLocked({ name: `tx/${name}`, locks: [ ...lineageLocks, { type: 'subtreeWrite', fp } ], fn: async () => {
      
      // Ensure all lineage Ents exist as Nodes, and resolve each lineage lock after the Node is
      // created
      // Consider that this is a bit early to be initiating the folder heirarchy, but currently a
      // bunch of operations use `this.fp.getLineage(trgFp)`; this implies that everything up until
      // `this.fp` already exists! If we weren't certain that a tx's root fp always existed, we
      // would have to do `filesystemRootFp.getLineage(trgFp)` instead. I think this change should
      // be made, as creating empty folders makes me sad. This change will require:
      // - `ensureLineageLocks` to expect a lineage always beginning from the system root
      // - `ensureLineageLocks` to have more efficient behaviour (e.g., check the top item for
      //   existence initially and if existing, immediately resolve all locks and short-circuit; or
      //   maybe just binary-search the lineage chain for the first non-existing node? in this case
      //   need to be careful to release the locks at the appropriate times)
      await this.lore.ensureLineageLocks(lineageLocks);
      
      const tx = new Scholar(this.lore, fp);
      try {
        const result = await fn(tx);
        await this.lore.remEmptyAncestors(fp.par());
        return result;
      } finally { tx.end(); }
      
    }});
    
  }
  async kid(fp: string[] | Fp) {
    
    // Returns `Promise<KidTransaction>`; example usage:
    //    | const kidTx = await rootTx.kid('C:/isolated');
    //    | // ... do a bunch of stuff with `kidTx` ...
    //    | kidTx.end();
    
    if (isCls(fp, Array)) fp = new Fp(fp);
    
    const kidPrm = Promise[later]<Scholar<L>>();
    this.transact({ name: 'kid', fp, fn: tx => {
      
      kidPrm.resolve(tx);
      
      const txDonePrm = Promise[later]();
      tx.endFns.push(() => txDonePrm.resolve());
      return txDonePrm;
      
    }});
    
    return kidPrm;
    
  }
  
  async getType(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getType', locks: [{ type: 'nodeRead', fp }], fn: () => this.lore.getType(fp) });
    
  }
  async getDataBytes(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getDataBytes', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      const stat = await this.lore.safeStat(fp);
      if (stat === null) return 0;
      if (stat.isFile()) return stat.size;
      
      // At this point `fp` is a directory; try to read the "~" file; any error results in 0 size
      const tildeStat = await this.lore.safeStat(fp.kid('~'));
      return tildeStat?.size ?? 0;
      
    }});
    
  }
  
  async setData(fp: Fp, data: null | string | Buffer | Obj<Json> | Json[]) {
    
    // TODO: Think about `await someTx.setData(someFp, 'null')` - this results in a file being
    // written with literal string content "null", but for `await someTx.getData(someFp, 'json')`,
    // it's impossible for the caller to tell whether the node is nonexistent in the system, or if
    // it does exist containing "null" (does this create any vulnerabilities??)
    
    this.checkFp(fp);
    
    const shouldRem = false
      || data === null
      || (isCls(data, String) && data.length === 0)
      || (isCls(data, Buffer) && data.length === 0);
    
    if (shouldRem) {
      
      // Writing `null` or empty string/buffer is implemented as a system-level delete - these
      // semantics are consistent; non-existing system reads return `null`, `''`, or `Buffer(0)`!
      
      return this.doLocked({ name: 'setLeafEmpty', locks: [{ type: 'nodeWrite', fp }], fn: async () => {
        
        const type = await this.lore.getType(fp);
        if (type === null) return;
        
        const unlinkFp = {
          leaf: () => fp,         // For leafs simply unlink the leaf
          node: () => fp.kid('~') // For nodes try to unlink the "~" child
        }[type]();
        
        await this.lore.remNode(unlinkFp);
        await this.lore.remEmptyAncestors(unlinkFp!.par());
        
      }});
      
    } else {
      
      // Setting a non-zero amount of data requires ensuring that all
      // ancestor nodes exist and finally writing the data
      
      const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }));
      const nodeLock = { type: 'nodeWrite', fp };
      
      return this.doLocked({ name: 'setData', locks: [ ...lineageLocks, nodeLock ], fn: async () => {
        
        await this.lore.setData(lineageLocks, fp, data);
        
      }});
      
    }
    
  }
  async getData(fp: Fp, enc: 'bin' | 'str' | 'json') {
    
    this.checkFp(fp);
    
    return this.doLocked({ name: 'getData', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      return this.lore.getData(fp, enc);
    }});
    
  }
  getDataHeadStream(fp: Fp) {
    
    // A "head stream" ingresses into an fp's data. Writing to a fp with an active "head stream"
    // succeeds, but silently overwrites the content; for this reason conflicts need to be detected
    
    this.checkFp(fp);
    
    const streamPrm = Promise[later]<Writable>();
    
    const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }));
    const nodeLock = { type: 'nodeWrite', fp };
    const prm = this.doLocked({ name: 'getHeadStream', locks: [ ...lineageLocks, nodeLock ], fn: async () => {
      
      // Ensure lineage
      const { stream, prm } = await this.lore.getHeadStreamAndFinalizePrm(lineageLocks, fp);
      
      // Expose the stream immediately
      streamPrm.resolve(stream);
      
      // Don't allow `doLocked` to finish until the stream is finalized (need to maintain locks)
      await prm;
      
    }});
    
    // Expose the head stream, with a "prm" attribute attached which can allow the consumer to
    // await operation completion
    return streamPrm.then(stream => Object.assign(stream, { prm }));
    
  }
  getDataTailStream(fp: Fp) {
    
    // A "tail stream" egresses from a file pointer's data storage. Once a stream has initialized,
    // it seems unaffected even if the file pointer is changed partway through - this means we can
    // release locks (allow the `doLocked` async fn to resolve) immediately!
    
    this.checkFp(fp);
    
    const streamPrm = Promise[later]<Readable>();
    
    const nodeLock = { type: 'nodeRead', fp };
    const prm = this.doLocked({ name: 'getTailStream', locks: [ nodeLock ], fn: async () => {
      
      const { stream, prm } = await this.lore.getTailStreamAndFinalizePrm(fp);
      streamPrm.resolve(stream); // Pass the initialized stream to the caller
      await prm;
      
    }});
    
    return streamPrm.then(stream => Object.assign(stream, { prm }));
    
  }
  
  async getKidNames(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getKidNames', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      return this.lore.getKidCmps(fp);
      
    }});
    
  }
  async remSubtree(fp: Fp) {
    
    this.checkFp(fp);
    
    return this.doLocked({ name: 'remSubtree', locks: [{ type: 'subtreeWrite', fp }], err: Error(''), fn: async () => {
      return this.lore.remSubtree(fp);
    }});
    
  }
  async iterateNode(fp: Fp, { bufferSize=150 }={}) {
    
    this.checkFp(fp);
    
    const itPrm = Promise[later]<PartialIterator<string>>();
    
    const prm = this.doLocked({ name: 'iterateNode', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      const iterator = await this.lore.getKidIteratorAndFinalizePrm(fp, { bufferSize });
      itPrm.resolve(iterator);
      await iterator.prm;
      
    }});
    
    const tx = this;
    return itPrm.then(it => ({
      
      async* [Symbol.asyncIterator]() {
        
        for await (const fd of it)
          yield new Fact(tx, fp.kid(fd));
        
      },
      close() { return it.close(); },
      prm
      
    }));
    
  }
  
  end() {
    
    // TODO: what happens when this is called on a Tx with some KidTxs?? We can't safely end the
    // Par until all the Kids are ended...
    return this.doLocked({ name: 'deactivate', locks: [], fn: async () => {
      this.active = true;
      for (const fn of this.endFns) fn();
    }});
    
  }
  
};

export class Fact {
  
  // I think the rightful philosophy is to deny traversal from Kid -> Par; to access a shallower
  // Ent, need access to some Ent shallow enough to contain that Ent - note a `par` function
  // exists but doesn't traverse any shallower than the Tx backing the Ent (note consumers could
  // already use `new Ent(ent.tx.fp).kid(...)` to access anything within the transaction)
  
  public fp: Fp;
  public tx: Scholar<Lore>;
  
  constructor(tx: Scholar<Lore>, fp: string[] | Fp) {
    
    if (isCls(fp, Array)) fp = new Fp(fp);
    for (const cmp of fp.cmps) if (/^[~]+$/.test(cmp)) throw Error('Illegal cmp must include char other than "~"')[mod]({ fp, cmp });
    
    this.fp = fp;
    this.tx = tx;
    
  }
  
  getCmps() { return this.fp.cmps; }
  
  kid(relFp: string[]): Fact;
  kid(relFp: string[], enc: {}): Fact;
  kid(relFp: string[], enc: { newTx: false }): Fact;
  kid(relFp: string[], enc: { newTx: true }): Promise<Fact>;
  kid(relFp: string[], enc?: { newTx?: boolean }): Promise<Fact> | Fact {
    
    const kidFp = this.fp.kid(relFp);
    if (enc?.newTx) {
      
      return this.tx.kid(kidFp).then(tx => new Fact(tx, kidFp)) as Promise<Fact> as any;
      
    } else {
      
      return new Fact(this.tx, kidFp);
      
    }
    
  }
  par(): Fact {
    
    if (this.tx.fp.equals(this.fp)) throw Error('parent is outside transaction');
    
    return new Fact(this.tx, this.fp.par());
    
  }
  
  // Data
  async getData(enc: 'str'):  Promise<string>;
  async getData(enc: 'bin'):  Promise<Buffer>;
  async getData(enc: 'json'): Promise<null | Obj<Json> | Json[]>;
  async getData(enc: 'bin' | 'str' | 'json') {
    return this.tx.getData(this.fp, enc);
  }
  
  // When opts are omitted, Buffer becomes an option, and strings remain in utf8
  async setData(data: null | string | Buffer | Obj<Json> | Json[]) {
    return this.tx.setData(this.fp, data);
  }
  async getDataBytes() { return this.tx.getDataBytes(this.fp); }
  async exists() { return this.getDataBytes().then(v => v > 0); }
  async rem() { return this.tx.remSubtree(this.fp); }
  async getDataHeadStream() { return this.tx.getDataHeadStream(this.fp); }
  async getDataTailStream() { return this.tx.getDataTailStream(this.fp); }
  async getKids(): Promise<Obj<Fact>> {
    const names = await this.tx.getKidNames(this.fp);
    return names[toObj](name => [ name, this.kid([ name ]) ]);
  }
  kids() {
    return this.tx.iterateNode(this.fp);
  }
  toString() { return this.fp.toString(); }
  fsp() { return this.fp.fsp(); }
  
};
