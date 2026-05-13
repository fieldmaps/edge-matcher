declare module "mproj" {
  const mproj: {
    internal: Record<string, unknown>;
    pj_init: (defn: string) => unknown;
    pj_add: (cb: (P: { a: number }) => void, name: string, label?: string) => void;
    [key: string]: unknown;
  };
  export default mproj;
}
