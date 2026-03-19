import nodeFs from 'node:fs';
import '@gershy/clearing';

const { safe, inCls } = cl;
const mod:   typeof cl.mod   = cl.mod;
const map:   typeof cl.map   = cl.map;
const slice: typeof cl.slice = cl.slice;

export const wrapFsError = (anchorErr: any, opts: { cause: any, name: string, fsp: string }) => {
  throw anchorErr[mod]({ cause: opts.cause, msg: `Failed low-level ${opts.name} on "${opts.fsp}"` });
};
export default (() => {
  
  type FsPrm = (typeof nodeFs)['promises'] & Pick<typeof nodeFs, 'createReadStream' | 'createWriteStream'>;
  return ({ ...nodeFs.promises, ...nodeFs[slice]([ 'createReadStream', 'createWriteStream' ]) } as FsPrm)
  
    [map]((fsProp: any, name): any => {
      
      // Include any non-function members of node:fs as-is
      if (!inCls(fsProp, Function)) return fsProp;
      
      // Functions become wrapped for better error reporting (especially stacktrace)
      return (...args) => {
        
        const err = Error();
        return safe(() => fsProp(...args), cause => wrapFsError(err, {
          cause,
          name,
          fsp: args[0] // In fs, the 1st arg is almost always the filepath
        }));
        
      };
      
    }) as FsPrm;
  
})();