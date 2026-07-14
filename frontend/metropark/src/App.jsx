import { AppShell } from "./components/layout/AppShell";
import { AppRouter } from "./routes/AppRouter";

function App() {
  return (
    <AppShell>
      <AppRouter />
    </AppShell>
  );
}

export default App;
