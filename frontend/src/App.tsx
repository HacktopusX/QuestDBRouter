import { Terminal } from "@/components/terminal/terminal";
import { StreamProvider } from "@/providers/StreamProvider";
import { TerminalProvider } from "@/providers/TerminalProvider";

export default function App() {
  return (
    <StreamProvider>
      <TerminalProvider>
        <Terminal />
      </TerminalProvider>
    </StreamProvider>
  );
}
