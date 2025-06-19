from framework.interfaces import Notifier
import requests
import os
import urllib3
import logging
import re
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

def normalize_marker(text):
    """Normalize marker by removing markdown header chars, spaces, and lowercasing."""
    return re.sub(r'^[#*\s]+', '', (text or '')).strip().lower()

class GitLabMRCommentNotifier(Notifier):
    def __init__(self, token, base_url, marker):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.headers = {"PRIVATE-TOKEN": self.token}
        self.marker = marker
        self.dry_run = os.getenv("DRY_RUN", "0") == "1" or os.getenv("DISABLE_MR_COMMENTS", "0") == "1"

    def notify(self, context, analysis_results):
        project_id = context['project_id']
        mr_iid = context['mr_iid']
        # Combine all analyzer results, separated by a horizontal rule
        content = "\n\n---\n\n".join(str(v) for v in analysis_results.values() if v)
        if self.marker:
            content = f"## {self.marker}\n\n{content}"

        if self.dry_run:
            logger.info(f"[DRY RUN] Would post to MR {mr_iid} in project {project_id} with marker '{self.marker}':\n{content}")
            return

        base_url = str(self.base_url)
        notes_url = f"{base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes"
        headers = self.headers
        logger.info(f"Fetching notes from URL: {notes_url}")
        # 1. Get all notes for the MR
        resp = requests.get(notes_url, headers=headers, verify=False)
        logger.info(f"Response status code: {resp.status_code}")
        #logger.info(f"Response content: {resp.text}")
        resp.raise_for_status()
        notes = resp.json()

        # 2. Look for an existing bot comment (by marker)
        existing_note = None
        for note in notes:
            if normalize_marker(self.marker) in normalize_marker(note.get("body", "")):
                existing_note = note
                break

        if existing_note:
            # 3. Update the existing comment
            note_id = existing_note["id"]
            update_url = f"{notes_url}/{note_id}"
            logger.info(f"Updating note at URL: {update_url}")
            resp = requests.put(update_url, headers=headers, json={"body": content}, verify=False)
            logger.info(f"Response status code: {resp.status_code}")
            #logger.info(f"Response content: {resp.text}")
            resp.raise_for_status()
            return resp.json()
        else:
            # 4. Create a new comment
            logger.info(f"Creating new note at URL: {notes_url}")
            resp = requests.post(notes_url, headers=headers, json={"body": content}, verify=False)
            logger.info(f"Response status code: {resp.status_code}")
            logger.info(f"Response content: {resp.text}")
            resp.raise_for_status()
            return resp.json() 