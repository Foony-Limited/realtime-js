import { describe, expect, it } from 'vitest';

import { ErrorCode } from './wire.js';

describe('wire', () => {
  it('exposes the documented error codes', () => {
    expect(ErrorCode.BadFrame).toBe(40001);
    expect(ErrorCode.BadAuth).toBe(40101);
    expect(ErrorCode.AuthExpired).toBe(40102);
    expect(ErrorCode.Forbidden).toBe(40300);
    expect(ErrorCode.NotFound).toBe(40400);
    expect(ErrorCode.Server).toBe(50000);
  });
});
