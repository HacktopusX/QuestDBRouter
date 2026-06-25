import type { IlpRow } from "@/lib/ilp/types";
import { tagsToMap } from "@/lib/ilp/types";

export function deriveTopicFromRow(row: IlpRow): string {
  const tags = tagsToMap(row.tags);
  return tags.get("symbol") ?? row.measurement;
}

export function normalizeTopic(topic: string): string {
  return topic.trim().toLowerCase();
}
