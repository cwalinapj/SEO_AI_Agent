PATCH_INSTRUCTIONS_FOR_repo_summarizer_with_memory.txt
======================================================

Goal
----
Prevent JSONDecodeError by:
- enforcing stricter JSON-only output
- extracting JSON object from model output when it includes extra tokens
- retrying a couple times on parse failures

Edits
-----

1) Add these imports near the top:
   import re
   from typing import Optional

2) Add this helper function somewhere above call_llm_json:

   def _extract_first_json_object(text: str) -> Optional[str]:
       \"\"\"Best-effort extraction of the first top-level JSON object from text.\"\"\"
       # Fast path: already looks like JSON object
       t = text.strip()
       if t.startswith("{") and t.endswith("}"):
           return t

       # Find a balanced {...} block
       start = t.find("{")
       if start == -1:
           return None

       depth = 0
       in_str = False
       esc = False
       for i in range(start, len(t)):
           ch = t[i]
           if in_str:
               if esc:
                   esc = False
               elif ch == "\\":
                   esc = True
               elif ch == '"':
                   in_str = False
           else:
               if ch == '"':
                   in_str = True
               elif ch == "{":
                   depth += 1
               elif ch == "}":
                   depth -= 1
                   if depth == 0:
                       return t[start : i + 1]
       return None

3) Replace your existing call_llm_json with this version (retry + salvage):

   def call_llm_json(client: OpenAI, model: str, instructions: str, payload: Dict[str, Any]) -> Dict[str, Any]:
       \"\"\"Ask model for JSON and robustly parse it.\"\"\"
       last_err = None
       for attempt in range(1, 4):  # up to 3 attempts
           resp = client.responses.create(
               model=model,
               instructions=instructions + "\\n\\nIMPORTANT: Output ONLY valid JSON. No extra text.",
               input=json.dumps(payload, ensure_ascii=False),
           )
           text = (resp.output_text or "").strip()

           # Attempt 1: direct parse
           try:
               data = json.loads(text)
               if isinstance(data, dict):
                   return data
           except Exception as e:
               last_err = e

           # Attempt 2: salvage first JSON object
           extracted = _extract_first_json_object(text)
           if extracted:
               try:
                   data = json.loads(extracted)
                   if isinstance(data, dict):
                       return data
               except Exception as e:
                   last_err = e

           # Retry with a stronger nudge on attempts 2/3
           instructions = (
               instructions
               + "\\n\\nYour previous output was invalid JSON. Return ONLY a single JSON object with double quotes."
           )

       # If all attempts fail:
       preview = text[:600] if 'text' in locals() else ''
       raise RuntimeError(
           f"Model did not return valid JSON after retries. Last error: {last_err}. Preview:\\n{preview}"
       )

4) (Optional but recommended) Reduce likelihood of giant JSON strings:
   In summarize_file_structured(), add:
     - "Keep summary_md under ~15 lines" (already have)
     - Also cap content length for the prompt: already done via MAX_FILE_CHARS_DEFAULT

5) Re-run step 1:
   python3 ./scripts/repo_summarizer_with_memory.py --root . --db .repo_index.sqlite --out REPO_SUMMARY.md --jsonl repo_index.jsonl

End of file
-----------
