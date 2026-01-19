from bs4 import BeautifulSoup


def ls(html_filepath: str) -> tuple[list[str], list[str]]:
    with open(html_filepath) as f:
        web = f.read()
    soup = BeautifulSoup(web, "lxml")

    files = []
    dirs = []
    for link in soup.find_all("a"):
        if link.has_attr("href") and link.string != "Parent Directory":
            text = link.string
            if text.endswith("/"):
                dirs.append(text)
            else:
                files.append(link['href'])
    return files, dirs

__all__ = ['ls']