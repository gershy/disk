import './main.ts';
import fs from 'node:fs/promises';
import { assertEqual, cmpAny, testRunner } from '../build/utils.test.ts';
import { Fact, rootFact } from './main.ts';

// Type testing
(async () => {
  
  type Enforce<Provided, Expected extends Provided> = { provided: Provided, expected: Expected };
  
  type Tests = {
    1: Enforce<{ x: 'y' }, { x: 'y' }>,
  };
  
})();

const isolated = async (fn: (fact: Fact) => Promise<void>) => {
  
  let fact: null | Fact = null;
  try {
    
    fact = await rootFact.kid([ import.meta.dirname, '.isolatedTest' ], { newTx: true });
    await fn(fact);
    
  } finally {
    
    await fact?.rem();
    fact?.tx.end();
    
  }
  
};
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
testRunner([
  
  { name: 'basic string data', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'data' ]);
    await data.setData('hello');
    const val = await data.getData('str');
    
    assertEqual(val, 'hello');
    
  })},
  { name: 'basic json data',fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'data' ])
    await data.setData({ a: 'b', x: 'y' });
    const val = await data.getData('json');
    
    assertEqual(val, { a: 'b', x: 'y' });
    
  })},
  { name: 'basic overwrite', fn: () => isolated(async fact => {
    
    const kid = fact.kid([ 'val' ]);
    await Promise.all([
      
      kid.setData({ a: 1, b: 2 }),
      kid.setData({ a: 1, b: 3 }),
      kid.setData({ a: 1, b: 4 }),
      kid.setData({ a: 1, b: 5 }),
      kid.setData({ a: 1, b: 6 }),
      
    ]);
    
    const val = await kid.getData('json');
    
    assertEqual(val, { a: 1, b: 6 });
    
  })},
  { name: 'leaf to node conversion', fn: () => isolated(async fact => {
    
    const par = fact.kid([ 'par' ]);
    const kid = par .kid([ 'kid' ]);
    await par.setData({ desc: 'par node' });
    await kid.setData({ desc: 'kid node' });
    
    const parData = await par.getData('json');
    const kidData = await kid.getData('json');
    
    assertEqual({ parData, kidData }, {
      parData: { desc: 'par node' },
      kidData: { desc: 'kid node' }
    });
    
  })},
  { name: 'data head stream', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'data' ]);
    const headStream = await data.getDataHeadStream();
    
    // Note this read is expected to wait for the head stream to be ended
    const valPrm = data.getData('str');
    await sleep(5);
    
    headStream.write('111');
    await sleep(5);
    
    headStream.write('222');
    await sleep(5);
    
    headStream.write('333');
    headStream.end();
    await sleep(5);
    
    const val = await valPrm;
    
    assertEqual(val, '111222333');
    
  })},
  { name: 'data head stream on directory', fn: () => isolated(async fact => {
    
    const par = fact.kid([ 'dir' ]);
    
    // Force `par` to be a dir
    await par.kid([ 'kid' ]).setData('this is a kid');
    
    const headStream = await par.getDataHeadStream();
    headStream.write('[streamed]');
    headStream.write('[into]');
    headStream.write('[dir]');
    headStream.end();
    
    assertEqual(await par.getData('str'), '[streamed][into][dir]');
    
  })},
  { name: 'data head stream with no content leaves no traces', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'par1', 'par2', 'par3', 'data' ]);
    const fsp = data.fsp();
    const headStream = await data.getDataHeadStream();
    headStream.end();
    
    assertEqual(await data.getData('json'), null);
    assertEqual(
      await fs.stat(fsp).then(
        val => ({ success: true, stat: val }),
        err => ({ success: false, err })
      ),
      {
        success: false,
        err: Error(`ENOENT: no such file or directory, stat '${fsp}'`)[mod]({
          code: 'ENOENT',
          errno: cmpAny,
          syscall: cmpAny,
          path: fsp
        })
      }
    );
    
  })},
  { name: 'data head stream missing parent dirs', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'par1', 'par2', 'par3', 'data' ]);
    const headStream = await data.getDataHeadStream();
    headStream.write('[streamed]');
    headStream.write('[into]');
    headStream.write('[dir]');
    headStream.end();
    
    assertEqual(
      await data.getData('str'),
      '[streamed][into][dir]'
    );
    
  })},
  { name: 'data tail stream', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'data' ]);
    await data.setData('abc'.repeat(1000));
    
    const readStream = await data.getDataTailStream();
    const chunks: any[] = [];
    readStream.on('data', d => chunks.push(d));
    await readStream.prm;
    
    assertEqual(Buffer.concat(chunks).toString('utf8'), 'abc'.repeat(1000));
    
  })},
  { name: 'get kids', fn: () => isolated(async fact => {
    
    await Promise.all(
      (50)[toArr](v => fact.kid([ 'par', `kid${v}` ]).setData(v.toString(10)))
    );
    
    const kids = await fact.kid([ 'par' ]).getKids();
    assertEqual(
      kids[map](kid => kid.toString()),
      (50)[toObj](v => [ `kid${v}`, `${fact.toString()}/par/kid${v}`
    ]));
    
  })},
  { name: 'iterate kids', fn: () => isolated(async fact => {
    
    await Promise.all(
      (50)[toArr](v => fact.kid([ 'par', `kid${v}` ]).setData(v.toString(10)))
    );
    
    const kids: Fact[] = [];
    for await (const kid of await fact.kid([ 'par' ]).kids())
      // Note there are no guarantees for iteration order
      kids.push(kid);
    
    assertEqual(
      new Set(kids[map](kid => kid.toString())),
      new Set((50)[toArr](v => `${fact.toString()}/par/kid${v}`))
    );
    
  })},
  { name: 'iterate kids with interrupt', fn: () => isolated(async fact => {
    
    await Promise.all(
      (50)[toArr](v => fact.kid([ 'par', `kid${v}` ]).setData(v.toString(10)))
    );
    
    const kids: Fact[] = [];
    const kidIt = await fact.kid([ 'par' ]).kids();
    let cnt = 0;
    for await (const kid of kidIt) {
      kids.push(kid);
      if (++cnt >= 30) break;
    }
    await kidIt.close();
    
    // Note there are no guarantees for iteration order - comparing size only
    assertEqual(kids.length, 30);
    
  })},
  { name: 'encoding', fn: () => isolated(async fact => {
    
    const data = fact.kid([ 'data' ]);
    
    const assertEmptyAllEncodings = async () => {
      
      const vals = await Promise[allObj]({
        str:  data.getData('str'),
        bin:  data.getData('bin'),
        json: data.getData('json')
      });
      assertEqual(vals, {
        str:  '',
        bin:  Buffer.alloc(0),
        json: null
      });
      
    };
    
    await assertEmptyAllEncodings();
    
    await data.setData('[1,2,3,{"x":"y"}]');
    assertEqual(
      await Promise[allObj]({
        str:  data.getData('str'),
        bin:  data.getData('bin'),
        json: data.getData('json')
      }),
      {
        str: '[1,2,3,{"x":"y"}]',
        bin: Buffer.from('[1,2,3,{"x":"y"}]'),
        json: [ 1, 2, 3, { x: 'y' } ]
      }
    );
    
    await data.setData('');
    await assertEmptyAllEncodings();
    
    await data.setData(Buffer.from([ 0, 1, 2, 3, 4, 5 ]));
    assertEqual(
      await Promise[allObj]({
        str:  data.getData('str'),
        bin:  data.getData('bin'),
        json: data.getData('json').then(
          val => ({ success: true, val }),
          err => ({ success: false, err })
        )
      }),
      {
        str: '\u0000\u0001\u0002\u0003\u0004\u0005',
        bin: Buffer.from([ 0, 1, 2, 3, 4, 5 ]),
        json: {
          success: false,
          err: Error('Failed locked op: "getData"')[mod]({
            cause: new Error('non-json')
          })
        }
      }
    );
    
    await data.setData(Buffer.alloc(0));
    await assertEmptyAllEncodings();
    
    await data.setData('hellooo');
    assertEqual(
      await data.getData('bin'),
      Buffer.from('hellooo')
    );
    
    await data.setData(null);
    await assertEmptyAllEncodings();
    
  })}
  
]);