import type { LayoutMode } from "@/types/terminal";

/** Fixed grid cell for each panel index — layout never changes on fullscreen. */
export const PANEL_SLOTS: Record<LayoutMode, string[]> = {
  "4": [
    "col-start-1 row-start-1",
    "col-start-2 row-start-1",
    "col-start-1 row-start-2",
    "col-start-2 row-start-2",
  ],
  "6": [
    "col-start-1 row-start-1",
    "col-start-2 row-start-1",
    "col-start-1 row-start-2",
    "col-start-2 row-start-2",
    "col-start-1 row-start-3 lg:col-start-3 lg:row-start-1",
    "col-start-2 row-start-3 lg:col-start-3 lg:row-start-2",
  ],
  "9": [
    "col-start-1 row-start-1",
    "col-start-2 row-start-1",
    "col-start-3 row-start-1",
    "col-start-1 row-start-2",
    "col-start-2 row-start-2",
    "col-start-3 row-start-2",
    "col-start-1 row-start-3",
    "col-start-2 row-start-3",
    "col-start-3 row-start-3",
  ],
};

export const LAYOUT_GRID: Record<LayoutMode, string> = {
  "4": "grid-cols-2 grid-rows-2",
  "6": "grid-cols-2 grid-rows-3 lg:grid-cols-3 lg:grid-rows-2",
  "9": "grid-cols-3 grid-rows-3",
};
