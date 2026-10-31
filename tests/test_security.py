import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ["MYSQLSH_PATH"] = sys.executable
import app as atlas  # noqa: E402


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.client = atlas.app.test_client()
        self.headers = {"Authorization": f"Bearer {atlas.ACCESS_TOKEN}"}

    def test_api_routes_require_run_capability(self):
        for path in ("/api", "/api/health", "/api/config", "/api/saved-connections", "/api/objects/example/data", "/api/objects/example/export"):
            self.assertEqual(self.client.get(path).status_code, 401)
            self.assertEqual(self.client.get(path, headers={"Authorization": "Bearer wrong"}).status_code, 401)
        self.assertEqual(self.client.post("/api/connect", json={}).status_code, 401)
        self.assertEqual(self.client.post("/api/sql/query", json={"sql": "SHOW TABLES"}).status_code, 401)
        self.assertEqual(self.client.get("/api/health", headers=self.headers).status_code, 200)
        with patch.object(atlas, "connected", True), patch.object(atlas, "get_data_slice", return_value={"rows": [{"marker": "operator-only"}]}) as data:
            response = self.client.get("/api/objects/example/data")
        self.assertEqual(response.status_code, 401)
        data.assert_not_called()

    def test_authorized_data_and_export_keep_working(self):
        with patch.object(atlas, "get_data_slice", return_value={"rows": [{"marker": "dummy"}]}) as data:
            response = self.client.get("/api/objects/example/data", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"][0]["marker"], "dummy")
        data.assert_called_once()
        with patch.object(atlas, "export_rows", return_value=("text/csv; charset=utf-8", "marker\ndummy\n", "csv")):
            response = self.client.get("/api/objects/example/export?format=csv", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"dummy", response.data)

    def test_sql_lab_rejects_file_output_and_unbounded_forms(self):
        allowed = (
            "SHOW TABLES;",
            "SELECT 'UPDATE' AS label LIMIT 1;",
            "WITH c AS (SELECT 1 AS n) SELECT n FROM c LIMIT 10 OFFSET 2",
            "SELECT * FROM entries LIMIT 500 OFFSET 100500",
            "/* ordinary comment */ SELECT COUNT(*) FROM entries LIMIT 1",
        )
        for sql in allowed:
            with self.subTest(sql=sql):
                self.assertEqual(atlas.assert_read_only_sql(sql), sql.strip())
        blocked = (
            "SELECT 'x' INTO OUTFILE '/tmp/dummy' LIMIT 1",
            "SELECT 'x' INTO/**/DUMPFILE '/tmp/dummy' LIMIT 1",
            "SELECT LOAD_FILE('/tmp/dummy') LIMIT 1",
            "SELECT * FROM t WHERE note = 'LIMIT 1'",
            "SELECT * FROM (SELECT * FROM t LIMIT 1) AS x",
            "SELECT * FROM t /* LIMIT 1 */",
            "SELECT * FROM t LIMIT 1001",
            "SELECT * FROM t LIMIT 1 OFFSET 18446744073709551616",
            "SELECT * FROM t LIMIT 1 UNION SELECT * FROM secret",
            "SELECT COUNT(*) FROM t",
            "SELECT 1 LIMIT 1; SELECT 2 LIMIT 1",
            "SELECT /*!50000 INTO OUTFILE '/tmp/dummy' */ 1 LIMIT 1",
            "SELECT 'a\\' INTO OUTFILE '/tmp/dummy' LIMIT 1",
        )
        for sql in blocked:
            with self.subTest(sql=sql), self.assertRaises(atlas.AppError):
                atlas.assert_read_only_sql(sql)
        with self.assertRaises(atlas.AppError) as error:
            atlas.assert_read_only_sql("SELECT 1 LIMIT " + "0" * 4301)
        self.assertEqual(error.exception.status, 400)

    def test_rejected_sql_never_reaches_mysqlsh(self):
        with patch.object(atlas, "run_mysqlsh_sql") as mysqlsh:
            with self.assertRaises(atlas.AppError):
                atlas.run_sql("SELECT 1 INTO OUTFILE '/tmp/dummy' LIMIT 1")
            mysqlsh.assert_not_called()

    def test_bounded_mysqlsh_process(self):
        command = [sys.executable, "-c", "import json; print(json.dumps({'rows':[{'value':'dummy'}]}))"]
        returncode, stdout, stderr = atlas.bounded_mysqlsh_process(command, "dummy-password", 5)
        self.assertEqual(returncode, 0)
        self.assertEqual(json.loads(stdout)["rows"][0]["value"], "dummy")
        self.assertEqual(stderr, "")
        with patch.object(atlas, "MAX_CLIENT_OUTPUT_BYTES", 32):
            with self.assertRaises(atlas.AppError) as error:
                atlas.bounded_mysqlsh_process([sys.executable, "-c", "print('x' * 10000)"], "dummy", 5)
        self.assertEqual(error.exception.status, 413)
        with self.assertRaises(atlas.AppError) as error:
            atlas.bounded_mysqlsh_process([sys.executable, "-c", "import time; time.sleep(5)"], "dummy", 0.1)
        self.assertEqual(error.exception.status, 504)
        with self.assertRaises(atlas.AppError) as error:
            atlas.bounded_mysqlsh_process([sys.executable, "-c", "import time; time.sleep(5)"], "x" * 1_000_000, 0.1)
        self.assertEqual(error.exception.status, 400)

    def test_client_failure_text_stays_in_local_diagnostics(self):
        with patch.object(atlas, "bounded_mysqlsh_process", return_value=(1, "", "dummy-secret")), patch.object(atlas.app.logger, "warning"):
            with self.assertRaises(atlas.AppError) as error:
                atlas.run_mysqlsh_sql(atlas.DEFAULT_CONFIG, "SELECT 1 LIMIT 1")
        self.assertNotIn("dummy-secret", error.exception.message)

    def test_mysqlsh_argument_and_password_contract_is_preserved(self):
        config = {**atlas.DEFAULT_CONFIG, "host": "dummy-host", "port": 3306, "user": "dummy-user", "database": "dummy-db", "sslMode": "PREFERRED", "password": "dummy-password"}
        with patch.object(atlas, "bounded_mysqlsh_process", return_value=(0, '{"rows": []}', "")) as process:
            atlas.run_mysqlsh_sql(config, "SELECT 1 LIMIT 1", timeout_seconds=17)
        command, password, timeout = process.call_args.args
        self.assertEqual(password, "dummy-password")
        self.assertEqual(timeout, 17)
        self.assertNotIn(password, command)
        self.assertEqual(command[-2:], ["-e", "SELECT 1 LIMIT 1"])

    def test_api_response_and_exception_do_not_disclose_dummy_secret(self):
        with patch.object(atlas, "get_overview", side_effect=RuntimeError("dummy-secret")), patch.object(atlas.app.logger, "exception"):
            response = self.client.get("/api/overview", headers=self.headers)
        self.assertEqual(response.status_code, 500)
        self.assertNotIn(b"dummy-secret", response.data)
        with patch.object(atlas, "MAX_API_RESPONSE_BYTES", 100), patch.object(atlas, "get_data_slice", return_value={"rows": [{"wide": "x" * 200}]}):
            response = self.client.get("/api/objects/example/data", headers=self.headers)
        self.assertEqual(response.status_code, 413)

    def test_semantic_index_validated_atomically_and_cache_reloaded(self):
        payload = {
            "version": atlas.SEMANTIC_INDEX_VERSION,
            "provider": "ollama",
            "model": "dummy-model",
            "documentCount": 1,
            "createdAt": "2026-10-31T00:00:00",
            "sourceHash": "a" * 64,
            "documents": [{"id": "note:1", "kind": "note", "title": "Dummy", "objectName": None, "sourceLabel": "notes.md", "text": "safe dummy text", "embedding": [1.0, 2.0], "norm": 2.236068}],
        }
        with tempfile.TemporaryDirectory() as directory:
            index = Path(directory) / "semantic_index.json"
            with patch.object(atlas, "RUNTIME_APPDATA", Path(directory)), patch.object(atlas, "SEMANTIC_INDEX_PATH", index):
                atlas.semantic_index_cache = None
                atlas.semantic_index_cache_stamp = None
                atlas.persist_semantic_index(payload)
                self.assertEqual(atlas.load_semantic_index()["documentCount"], 1)
                self.assertEqual(list(Path(directory).glob(".semantic-index-*")), [])
                invalid = {**payload, "documents": [{**payload["documents"][0], "embedding": [float("nan")]}]}
                with self.assertRaises(atlas.AppError):
                    atlas.persist_semantic_index(invalid)
                self.assertEqual(json.loads(index.read_text())["documentCount"], 1)
                index.write_text('{"version": 1, "documents": []}', encoding="utf-8")
                with self.assertRaises(atlas.AppError):
                    atlas.load_semantic_index()

    def test_note_headings_are_checked_before_embedding(self):
        with patch.object(atlas, "read_text_if_exists", return_value="## \nDummy body"), patch.object(atlas, "inferred_view_names", return_value=[]):
            documents = atlas.build_semantic_documents()
        self.assertEqual(documents[0]["title"], "Schema Notes")
        with self.assertRaises(atlas.AppError) as error:
            atlas.split_markdown_sections("## " + "x" * 501 + "\nDummy body")
        self.assertEqual(error.exception.status, 400)

    def test_model_receives_context_as_data_without_tools(self):
        status = {"ollamaReachable": True, "models": [{"id": "qwen3:8b", "installed": True, "resolvedName": "dummy-model"}]}
        captured = {}

        def fake_chat(model, messages):
            captured["messages"] = messages
            return {"message": {"content": "dummy answer"}}

        with patch.object(atlas, "ensure_connected"), patch.object(atlas, "get_llm_status", return_value=status), patch.object(atlas, "build_ask_atlas_context", return_value=("dummy database text", [])), patch.object(atlas, "ollama_chat", side_effect=fake_chat):
            atlas.ask_atlas("qwen3:8b", [{"role": "user", "content": "question"}])
        self.assertEqual(captured["messages"][1]["role"], "user")
        self.assertIn("untrusted text", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
