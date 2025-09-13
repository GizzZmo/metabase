import { useEffect, useRef } from "react";

/**
 * Custom hook that provides a ref which automatically scrolls the referenced element into view on mount.
 * 
 * @returns A ref object to be attached to a DOM element that should be scrolled into view
 */
export const useScrollOnMount = () => {
  const ref = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (ref.current) {
      ref.current.scrollIntoView?.({ block: "center" });
    }
  }, []);

  return ref;
};
