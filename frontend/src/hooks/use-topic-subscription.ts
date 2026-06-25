import { useEffect, useRef } from "react";
import type { IlpRow, ReplayOut } from "@/lib/ilp/types";
import { useStream } from "@/providers/StreamProvider";

export function useTopicSubscription(
  topic: string,
  onRow: (row: IlpRow) => void,
  onReplay: (payload: ReplayOut) => void,
) {
  const { client } = useStream();
  const onRowRef = useRef(onRow);
  const onReplayRef = useRef(onReplay);
  onRowRef.current = onRow;
  onReplayRef.current = onReplay;

  useEffect(() => {
    if (!topic.trim()) return;
    const handlers = {
      onRow: (row: IlpRow) => onRowRef.current(row),
      onReplay: (payload: ReplayOut) => onReplayRef.current(payload),
    };
    client.addTopic(topic, handlers);
    return () => client.removeTopic(topic, false);
  }, [client, topic]);
}
