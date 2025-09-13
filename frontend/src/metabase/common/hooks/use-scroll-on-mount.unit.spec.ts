import { renderHook } from "@testing-library/react";

import { useScrollOnMount } from "./use-scroll-on-mount";

describe("useScrollOnMount", () => {
  const mockScrollIntoView = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should return a ref object", () => {
    const { result } = renderHook(() => useScrollOnMount());
    
    expect(result.current).toHaveProperty("current");
    expect(result.current.current).toBeNull();
  });

  it("should call scrollIntoView with correct options when ref has a current element", () => {
    const { result } = renderHook(() => useScrollOnMount());
    
    // Create a mock element and assign it to the ref
    const mockElement = {
      scrollIntoView: mockScrollIntoView,
    } as unknown as HTMLElement;
    
    result.current.current = mockElement;
    
    // Re-render to trigger the useEffect
    renderHook(() => useScrollOnMount());
    
    expect(mockScrollIntoView).toHaveBeenCalledWith({ block: "center" });
  });

  it("should not throw error when element does not have scrollIntoView method", () => {
    const { result } = renderHook(() => useScrollOnMount());
    
    // Create a mock element without scrollIntoView
    const mockElement = {} as HTMLElement;
    
    result.current.current = mockElement;
    
    // Should not throw error
    expect(() => renderHook(() => useScrollOnMount())).not.toThrow();
  });
});