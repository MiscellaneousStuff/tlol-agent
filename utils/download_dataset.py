# Set your HDD path
HDD_PATH="D:\\huggingface\\maknee\\"

from huggingface_hub import snapshot_download

snapshot_download(
    repo_id="maknee/league-of-legends-decoded-replay-packets",
    repo_type="dataset",
    local_dir=HDD_PATH,
    local_dir_use_symlinks=False,
    resume_download=True,  # resume if interrupted
)