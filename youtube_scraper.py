import json
from yt_dlp import YoutubeDL
from typing import Any
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

lock = Lock()
comments_folder = "comments"
youtube_data_csv = "youtube_data.csv"


class YouTubeVideo:
    def __init__(self, url: str) -> None:
        self.url = url
        self.metadata = self._get_metadata()
        
    def download_video(self):
        # Set options for the downloader
        opts = {
            'format': 'best',  # Choose the best quality format available
            'outtmpl': '%(title)s.%(ext)s'  # Output template for the filename
        }

        # Create a YoutubeDL object and download the video
        with YoutubeDL(opts) as yt:  # type: ignore
            yt.download([self.url])
    
    def _get_metadata(self) -> dict[str, Any]:
        opts = {
            "getcomments": True,
            "extractor_args": {
                "youtube": {
                    "max_comments": ["all", "all", "all", "all"],
                }
            }
        }
        with YoutubeDL(opts) as yt:  # type: ignore
            # extract information about the video
            info = yt.extract_info(self.url, download=False)

            id = info.get("id")
            title = info.get("title")
            description = info.get("description")
            uploader = info.get("uploader")
            uploader_id = info.get("uploader_id")
            upload_date = info.get("upload_date")
            duration = info.get("duration")
            view_count = info.get("view_count")
            like_count = info.get("like_count")
            dislike_count = info.get("dislike_count")
            comment_count = info.get("comment_count")
            thumbnail = info.get("thumbnail")
            # formats = info.get("formats")
            subtitles = info.get("subtitles")
            age_limit = info.get("age_limit")
            categories = info.get("categories")
            tags = info.get("tags")
            is_live = info.get("is_live")
            language = info.get("language")
            comments = info.get("comments")
            
            data = {
                "URL": self.url,
                "Id": id,
                "Upload_date": upload_date,
                "Title": title,
                "Description": description,
                "Uploader": uploader,
                "Uploader_id": uploader_id,
                "Duration": duration,
                "View_count": view_count,
                "Like_count": like_count,
                "Dislike_count": dislike_count,
                "Comment_count": comment_count,
                "Thumbnail": thumbnail,
                # "Formats": formats,
                "Subtitles": subtitles,
                "Age_limit": age_limit,
                "Categories": categories,
                "Tags": tags,
                "Is_live": is_live,
                "Language": language,
                "Comments": comments,
            }
        
        return data
    
    def __getitem__(self, key) -> Any:
        if key in self.metadata:
            return self.metadata[key]
        else:
            return None


def scrape_video(url: str):
    video = YouTubeVideo(url=url)
    with open(f"{comments_folder}/{video['Id']}.json", "w+") as f:
        json.dump(video["Comments"], f, ensure_ascii=False)
        print(f"Dumping {len(video['Comments'])}")
    
    del video.metadata["Comments"]
    data_to_write = video.metadata

    df = pd.DataFrame([data_to_write])

    # write data to CSV
    # with lock:
    #     df.to_csv(youtube_data_csv, mode="a", header=False, index=False)

if __name__ == "__main__":
    colossal_yt_links = [
        "https://www.youtube.com/watch?v=ExUKyZxqKpY"
        "https://www.youtube.com/watch?v=cZjvzqkI7hc",
        "https://www.youtube.com/watch?v=dulcAbkxLFI",
        "https://www.youtube.com/watch?v=kdsXWfDMKxI",
        "https://www.youtube.com/watch?v=jFSasvWJl4A",
        "https://www.youtube.com/watch?v=-PgLG6AquUM",
        "https://www.youtube.com/watch?v=ZUsqgh9tnAE",
        "https://www.youtube.com/watch?v=ExUKyZxqKpY",
        "https://www.youtube.com/watch?v=R1XdGQIJDC4",
        "https://www.youtube.com/watch?v=_L-ykYKbK3Y",
        "https://www.youtube.com/watch?v=b5HywS8c32Y",
        "https://www.youtube.com/watch?v=uqkYwEX6k38",
        "https://www.youtube.com/watch?v=JyJn_XyWeLI",
        "https://www.youtube.com/watch?v=RZpMSMHWDWE",
        "https://www.youtube.com/watch?v=PmAbyBtfLpc",
        "https://www.youtube.com/watch?v=tyGz4XSlTwI",
        "https://www.youtube.com/watch?v=ZulV25IMHqM",
        "https://www.youtube.com/watch?v=YLwAoT1aIrs",
        "https://www.youtube.com/watch?v=cS5sRa_oRio",
        "https://www.youtube.com/watch?v=71foem3F9m0",
        "https://www.youtube.com/watch?v=iqtcYTowtYs",
        "https://www.youtube.com/watch?v=CdZJl9HZsLw",
        "https://www.youtube.com/watch?v=pn59wSBgXOI",
        "https://www.youtube.com/watch?v=xpDURuMqgxs",
        "https://www.youtube.com/watch?v=KlcADQWX2U4",
        "https://www.youtube.com/watch?v=GO7Ygy8HZ7E",
        "https://www.youtube.com/watch?v=2501qT5bxYI",
        "https://www.youtube.com/watch?v=Mxfr9Zu_1X4",
        "https://www.youtube.com/watch?v=Dg2m2BTFU70",
        "https://www.youtube.com/watch?v=u3TJYiYB6nk",
        "https://www.youtube.com/watch?v=KjXNpQnwp4o",
        "https://www.youtube.com/watch?v=rNyXQHoMgHw",
        "https://www.youtube.com/watch?v=GJvPwbwjCLQ",
        "https://www.youtube.com/watch?v=Mv3I622Rnaw",
        "https://www.youtube.com/watch?v=hb3N2zwEeZ8",
        "https://www.youtube.com/watch?v=jrqbw2aMIIo",
        "https://www.youtube.com/watch?v=smMaIjFj0Mg",
        "https://www.youtube.com/watch?v=Xk9fY3_JBPA",
        "https://www.youtube.com/watch?v=uZUEEILgWz0",
        "https://www.youtube.com/watch?v=uzyjemkt1Ek",
        "https://www.youtube.com/watch?v=-XqcqbLBLHo",
        "https://www.youtube.com/watch?v=Y-W5bAZd8zk",
        "https://www.youtube.com/watch?v=JMv2sduMk7Q",
        "https://www.youtube.com/watch?v=R7HiJ79Vs8o",
        "https://www.youtube.com/watch?v=n2bJof6jgBM",
        "https://www.youtube.com/watch?v=erOn9XORIBw",
        "https://www.youtube.com/watch?v=hKlvyqMa5jM",
        "https://www.youtube.com/watch?v=1S6TutWO_tc",
        "https://www.youtube.com/watch?v=Ghih0iftWHw",
        "https://www.youtube.com/watch?v=oORvaYkFvpM",
        "https://www.youtube.com/watch?v=YzDRT-QcXZw",
        "https://www.youtube.com/watch?v=rcLDRjIVyqc",
        "https://www.youtube.com/watch?v=9JwJF1qRcD8",
        "https://www.youtube.com/watch?v=T2Jgypu3Kr4",
        "https://www.youtube.com/watch?v=xSMVGVuGNyI",
        "https://www.youtube.com/watch?v=rKlNvp1j_VA",
        "https://www.youtube.com/watch?v=bH-qb50AGYs",
        "https://www.youtube.com/watch?v=wuKWd9cPXsw",
        "https://www.youtube.com/watch?v=BUT_NllJv7U",
        "https://www.youtube.com/watch?v=BqU4i34BmEc",
        "https://www.youtube.com/watch?v=8OHC_eHmDo8",
        "https://www.youtube.com/watch?v=sdujfuawcpU",
        "https://www.youtube.com/watch?v=o4gjopiVFb0",
        "https://www.youtube.com/watch?v=OSE8C2PIobk",
        "https://www.youtube.com/watch?v=c3B_FE18ZTo",
        "https://www.youtube.com/watch?v=oQILsqK4iyU",
        "https://www.youtube.com/watch?v=Q5mdhcA3xmE",
        "https://www.youtube.com/watch?v=jVh_0vMboWA",
        "https://www.youtube.com/watch?v=cg5skhUStRI",
        "https://www.youtube.com/watch?v=lT_y8JC2l14",
        "https://www.youtube.com/watch?v=9pUmSSqcXSk",
        "https://www.youtube.com/watch?v=dsbf1XmNj2U",
        "https://www.youtube.com/watch?v=jQlCYV-gEmo",
        "https://www.youtube.com/watch?v=XApw9N8fQk8",
        "https://www.youtube.com/watch?v=rP5s0cFBZxw",
        "https://www.youtube.com/watch?v=Pz_jU8rGbaY",
        "https://www.youtube.com/watch?v=7ZOHaY-psQ0",
        "https://www.youtube.com/watch?v=S_IUTSWEMrE",
        "https://www.youtube.com/watch?v=ysV3ok6lNBA",
        "https://www.youtube.com/watch?v=GYMUbVXOXXI",
        "https://www.youtube.com/watch?v=WtkYX401Mm0",
        "https://www.youtube.com/watch?v=uyZNoxxe6IA",
        "https://www.youtube.com/watch?v=AO0j--_0C8A",
        "https://www.youtube.com/watch?v=dsijDGQi1_g",
        "https://www.youtube.com/watch?v=BkaBPMIrM_w",
        "https://www.youtube.com/watch?v=mT0eWbLApgk",
        "https://www.youtube.com/watch?v=G8zVJRHUse4",
        "https://www.youtube.com/watch?v=O4loaqgds0M",
        "https://www.youtube.com/watch?v=a_LyuietRZQ",
        "https://www.youtube.com/watch?v=luaToAASPLY",
        "https://www.youtube.com/watch?v=MiqBS_ZL1cU",
        "https://www.youtube.com/watch?v=-ECiIMZ2Ojo",
        "https://www.youtube.com/watch?v=Qf_bbGHjcRM",
        "https://www.youtube.com/watch?v=OZ_LamKK8ck",
        "https://www.youtube.com/watch?v=Pvaab1UsE48",
        "https://www.youtube.com/watch?v=jiXxvKCFq-M",
        "https://www.youtube.com/watch?v=uwQMretXr9Q",
        "https://www.youtube.com/watch?v=4Nt4N2yQt0E",
        "https://www.youtube.com/watch?v=ZAckGs_QNss",
        "https://www.youtube.com/watch?v=Y1PLBd6mPrI",
        "https://www.youtube.com/watch?v=PYsCeFJToqQ",
        "https://www.youtube.com/watch?v=TWF7--XlaKk",
        "https://www.youtube.com/watch?v=uBFfDPm6Juw",
        "https://www.youtube.com/watch?v=PvV896qE8Ks",
        "https://www.youtube.com/watch?v=nReJXXg9H18",
        "https://www.youtube.com/watch?v=pJIxAIQYUwM",
        "https://www.youtube.com/watch?v=9n2ExJZMWVc",
        "https://www.youtube.com/watch?v=20L-W8g_CoI",
        "https://www.youtube.com/watch?v=Dgw1smmAMGM",
        "https://www.youtube.com/watch?v=4jPXOtMk52c",
        "https://www.youtube.com/watch?v=vv5DKzLbvS8",
        "https://www.youtube.com/watch?v=-MBPxYNS0Cg",
        "https://www.youtube.com/watch?v=aD4-iOWTSWc",
        "https://www.youtube.com/watch?v=Qfbikkkjqh8",
        "https://www.youtube.com/watch?v=o3IhU5nOFEE",
        "https://www.youtube.com/watch?v=lSxIaMBHVfs",
        "https://www.youtube.com/watch?v=fJIl_R9xUuk",
        "https://www.youtube.com/watch?v=5MtiUoGmkzg",
        "https://www.youtube.com/watch?v=va9SmxL34Zk",
        "https://www.youtube.com/watch?v=NuFtKX0gI2E",
        "https://www.youtube.com/watch?v=NI0lxqGX3NU",
        "https://www.youtube.com/watch?v=U33mOXJJuUI",
        "https://www.youtube.com/watch?v=0QqQMzn2urk",
        "https://www.youtube.com/watch?v=7eALf8VsOFM",
        "https://www.youtube.com/watch?v=5UeXf-mKrDU",
        "https://www.youtube.com/watch?v=goDW82afQ54",
        "https://www.youtube.com/watch?v=6YN9X4tU3sA",
        "https://www.youtube.com/watch?v=88Gw5up948o",
        "https://www.youtube.com/watch?v=4qSSJUqqCwA",
        "https://www.youtube.com/watch?v=qNi2OCgybhc",
        "https://www.youtube.com/watch?v=O9nN9SIurJI",
        "https://www.youtube.com/watch?v=39Z2HbEnbPQ",
        "https://www.youtube.com/watch?v=FQxspJpfXEc",
        "https://www.youtube.com/watch?v=-8jdaT6XIxM",
        "https://www.youtube.com/watch?v=OJ0jEOdpEVI",
        "https://www.youtube.com/watch?v=i39hlmeGKSg",
        "https://www.youtube.com/watch?v=r_Ay7ACx45Q",
        "https://www.youtube.com/watch?v=zoJ76wYC2qc",
        "https://www.youtube.com/watch?v=57xe-hFUBV4",
        "https://www.youtube.com/watch?v=iyzqXV_PLbU",
        "https://www.youtube.com/watch?v=CY4LwvhAM18",
        "https://www.youtube.com/watch?v=pCK4Sc91aFQ",
        "https://www.youtube.com/watch?v=ywFzRQ9LDxA",
        "https://www.youtube.com/watch?v=IhD7L-GIpZU",
        "https://www.youtube.com/watch?v=uZP_Katrxy8",
        "https://www.youtube.com/watch?v=Lyz8qS6piQY",
        "https://www.youtube.com/watch?v=F5uCuOwK_VE",
        "https://www.youtube.com/watch?v=ukcmyvDf6wA",
        "https://www.youtube.com/watch?v=vPX4tm-J2bU",
        "https://www.youtube.com/watch?v=bFYohrLfaAM",
        "https://www.youtube.com/watch?v=21etAYg5CNM",
        "https://www.youtube.com/watch?v=B2i5oNXK2QE",
        "https://www.youtube.com/watch?v=-X0HD5MKsSA",
        "https://www.youtube.com/watch?v=uOKj0L_SPUc",
        "https://www.youtube.com/watch?v=3egYWFXrb5M",
        "https://www.youtube.com/watch?v=RE35cRpWT5A",
        "https://www.youtube.com/watch?v=BP0HJXTHaZ8",
        "https://www.youtube.com/watch?v=tN2tpDfmEVc",
        "https://www.youtube.com/watch?v=PtolgHWRXx4",
        "https://www.youtube.com/watch?v=2P43x2at4aM",
        "https://www.youtube.com/watch?v=2bb-7S1SLjc",
        "https://www.youtube.com/watch?v=TfpQniepJms",
        "https://www.youtube.com/watch?v=XeMn3AKVq1c",
        "https://www.youtube.com/watch?v=X2Z5ZtjBavs",
        "https://www.youtube.com/watch?v=yum-Gx0q3Zk",
        "https://www.youtube.com/watch?v=EtpLyr9Ma1w",
        "https://www.youtube.com/watch?v=lPfTfboBsdw",
        "https://www.youtube.com/watch?v=smWAhRArLN4",
        "https://www.youtube.com/watch?v=lK2H1QR5tJI",
        "https://www.youtube.com/watch?v=owKth3eAryU",
        "https://www.youtube.com/watch?v=-GjtqU2iz1E",
        "https://www.youtube.com/watch?v=Z3TDeV_YL1M",
        "https://www.youtube.com/watch?v=ZtQM2ehvcbY",
        "https://www.youtube.com/watch?v=KU4JvQAsnc0",
        "https://www.youtube.com/watch?v=ynbpiZ6l1KY",
        "https://www.youtube.com/watch?v=aObeB2gwG18",
        "https://www.youtube.com/watch?v=FGNBhLJQD3c",
        "https://www.youtube.com/watch?v=UzpbIvLFm3k",
        "https://www.youtube.com/watch?v=0a_0RX745bU",
        "https://www.youtube.com/watch?v=kbzgEBa_w70",
        "https://www.youtube.com/watch?v=4qEiwpg4XEk",
        "https://www.youtube.com/watch?v=Jg9LDxvQfys",
        "https://www.youtube.com/watch?v=-iQ1BfPL6Kw",
        "https://www.youtube.com/watch?v=lj2Vb1HWjew",
        "https://www.youtube.com/watch?v=Q8G3JCiU0Io",
        "https://www.youtube.com/watch?v=43APKOSg-ik",
        "https://www.youtube.com/watch?v=c6E9tTXZ328",
        "https://www.youtube.com/watch?v=2tFJLyX_xKU",
        "https://www.youtube.com/watch?v=33AFnAGZZ74",
        "https://www.youtube.com/watch?v=XCmJGm8o4yw",
        "https://www.youtube.com/watch?v=xy1BdfxDOak",
        "https://www.youtube.com/watch?v=nbiGLGTn2-0",
        "https://www.youtube.com/watch?v=pNXEj8vG-f8",
        "https://www.youtube.com/watch?v=BFfS5Zf8Pj4",
        "https://www.youtube.com/watch?v=FDmErstmzx8",
        "https://www.youtube.com/watch?v=8ZG7vfF-quw",
        "https://www.youtube.com/watch?v=WrPOZGPs9LM",
        "https://www.youtube.com/watch?v=nbgC9xdex9A",
        "https://www.youtube.com/watch?v=ZbuxJcVRQG4",
        "https://www.youtube.com/watch?v=DVJpVq4doto",
        "https://www.youtube.com/watch?v=tcMjpUClhwc",
        "https://www.youtube.com/watch?v=JavBKdZbc3Q",
        "https://www.youtube.com/watch?v=-g2rPEW3XJY",
        "https://www.youtube.com/watch?v=4endCuPXedE",
        "https://www.youtube.com/watch?v=uqYr--qmVI8",
        "https://www.youtube.com/watch?v=RLJ_-NYE9lk",
        "https://www.youtube.com/watch?v=xCvcSf0b7xo",
        "https://www.youtube.com/watch?v=fYnC5-OIrZw",
        "https://www.youtube.com/watch?v=xpiR9PMijDI",
        "https://www.youtube.com/watch?v=Mx2-ertAvNY",
        "https://www.youtube.com/watch?v=ZKzYa8clE7Q",
        "https://www.youtube.com/watch?v=VX3di2xpEoc",
        "https://www.youtube.com/watch?v=yyeHkKXoIhE",
        "https://www.youtube.com/watch?v=XHvt_hNoYbk",
        "https://www.youtube.com/watch?v=-xkDAH6JJUM",
        "https://www.youtube.com/watch?v=-e2GNeorQL8",
        "https://www.youtube.com/watch?v=Jelqet7qtDY",
        "https://www.youtube.com/watch?v=w9B1te9ddJg",
        "https://www.youtube.com/watch?v=UFpaQ4sdHb0",
        "https://www.youtube.com/watch?v=67Ao3aSGDq0",
        "https://www.youtube.com/watch?v=9roy4XxLAXc",
        "https://www.youtube.com/watch?v=MW5J60OwP7Q",
        "https://www.youtube.com/watch?v=HmGvr4rRVK0",
        "https://www.youtube.com/watch?v=MMITrWSF6j8",
        "https://www.youtube.com/watch?v=jH9oI8Zzv2E",
        "https://www.youtube.com/watch?v=N5lDIsDOzvo",
        "https://www.youtube.com/watch?v=cBViRZZ_lRw",
        "https://www.youtube.com/watch?v=YhZlX8N9jto",
        "https://www.youtube.com/watch?v=qYLVVjgFFcI",
        "https://www.youtube.com/watch?v=aTPYnO5J0Uo",
        "https://www.youtube.com/watch?v=G3Z3mKmBxgY",
        "https://www.youtube.com/watch?v=9a9Xi-VUDe8",
        "https://www.youtube.com/watch?v=6i2d8_Xwj88",
        "https://www.youtube.com/watch?v=r9WST1UYclw",
        "https://www.youtube.com/watch?v=1uzaOWlq0tA",
        "https://www.youtube.com/watch?v=VKkdc-cH4IQ",
        "https://www.youtube.com/watch?v=cSAiGpebZl4",
        "https://www.youtube.com/watch?v=Nphjj-PMQDY",
        "https://www.youtube.com/watch?v=g_0IcLY7lDo",
        "https://www.youtube.com/watch?v=a6_Ljvm4KRo",
        "https://www.youtube.com/watch?v=8dLiPnfYynY",
        "https://www.youtube.com/watch?v=narFUmoC3oc",
        "https://www.youtube.com/watch?v=cZ-ikEeXuIE",
        "https://www.youtube.com/watch?v=_uZeqrST0rg",
        "https://www.youtube.com/watch?v=2Q-4PyLtSyU",
        "https://www.youtube.com/watch?v=EhZoq_znrFw",
        "https://www.youtube.com/watch?v=Sq5IQgg7Dmo",
        "https://www.youtube.com/watch?v=Xx-Oip3NSUU",
        "https://www.youtube.com/watch?v=pkk3vk5AYYk",
        "https://www.youtube.com/watch?v=xE-Zo7k8Dpc",
        "https://www.youtube.com/watch?v=HFS5BncWxh0",
        "https://www.youtube.com/watch?v=e5u1HQoeXAs",
        "https://www.youtube.com/watch?v=QfNLqSqi6wk",
        "https://www.youtube.com/watch?v=TjoLspqhmSI",
        "https://www.youtube.com/watch?v=6I8KS7b70qM",
        "https://www.youtube.com/watch?v=Y--L-_BQ950",
        "https://www.youtube.com/watch?v=VOUNzx6I7ug",
        "https://www.youtube.com/watch?v=q79z0gUMsoc",
        "https://www.youtube.com/watch?v=MdQhLE2o3Cs",
        "https://www.youtube.com/watch?v=y6OUkbcDci8",
        "https://www.youtube.com/watch?v=CSohYhiZxr0",
        "https://www.youtube.com/watch?v=0clnTYdzc-0",
        "https://www.youtube.com/watch?v=NA9t9g07H4A",
        "https://www.youtube.com/watch?v=KhzMp-xR9nQ",
        "https://www.youtube.com/watch?v=nDkifXDqTS8",
        "https://www.youtube.com/watch?v=uTNFeAEmRbM",
        "https://www.youtube.com/watch?v=PjccVG_Vwjo",
        "https://www.youtube.com/watch?v=5nf16nzI6IY",
        "https://www.youtube.com/watch?v=0cSjm1pURTA",
        "https://www.youtube.com/watch?v=FKQjVZ-zYbo",
        "https://www.youtube.com/watch?v=ckYrSEmFa3M",
        "https://www.youtube.com/watch?v=mdi3Um3BPyw",
        "https://www.youtube.com/watch?v=xy6GGb2NEH8",
        "https://www.youtube.com/watch?v=P0snFEhhIko",
        "https://www.youtube.com/watch?v=EH38qyZvE3c",
        "https://www.youtube.com/watch?v=_-BmJslhcNU",
        "https://www.youtube.com/watch?v=N1-7DdG1N7c",
        "https://www.youtube.com/watch?v=DA1zHezXs4Y",
        "https://www.youtube.com/watch?v=WOn1QSOpEuI",
        "https://www.youtube.com/watch?v=ZIRR8Wg_9Tc",
        "https://www.youtube.com/watch?v=XP9D_abiDOQ",
        "https://www.youtube.com/watch?v=jDDBV3ysHRM",
        "https://www.youtube.com/watch?v=liQINHS21R4",
        "https://www.youtube.com/watch?v=m9gtLHP8lkM",
        "https://www.youtube.com/watch?v=wUI0MsShsh0",
        "https://www.youtube.com/watch?v=aZ9OTsB6nx0",
        "https://www.youtube.com/watch?v=ByPv7Rda4Lk",
        "https://www.youtube.com/watch?v=OnBKVtjpTtE",
        "https://www.youtube.com/watch?v=eeRFZPb0-VA",
        "https://www.youtube.com/watch?v=Ef-VytD8wcg",
        "https://www.youtube.com/watch?v=OZl6NhqHeUw",
        "https://www.youtube.com/watch?v=kuSzmXo3uJU",
        "https://www.youtube.com/watch?v=kuWNQyOkMAo",
        "https://www.youtube.com/watch?v=xEPXsxgRmBs",
        "https://www.youtube.com/watch?v=ZnWUKKDLe-w",
        "https://www.youtube.com/watch?v=voTfSJw7Z1M",
        "https://www.youtube.com/watch?v=sfg9rjy8me0",
        "https://www.youtube.com/watch?v=8Vp9Y_NpdmA",
        "https://www.youtube.com/watch?v=_0XukmXBDjc",
        "https://www.youtube.com/watch?v=tTbAUoNAhkQ",
        "https://www.youtube.com/watch?v=6VOZw6xmd2w",
        "https://www.youtube.com/watch?v=8UnOsOjG7Dc",
        "https://www.youtube.com/watch?v=QtklK16WmJU",
        "https://www.youtube.com/watch?v=pUR4AqLDiVM",
        "https://www.youtube.com/watch?v=O1aeG6PHO7c",
        "https://www.youtube.com/watch?v=RtXjsE_Jv00",
        "https://www.youtube.com/watch?v=JQZzyADUNvU",
        "https://www.youtube.com/watch?v=2HbKwsAChyQ",
        "https://www.youtube.com/watch?v=UaXL24rDX6w",
        "https://www.youtube.com/watch?v=Q5_8ulDPHpg",
        "https://www.youtube.com/watch?v=LpHRP1U_BBM",
        "https://www.youtube.com/watch?v=KK1bXbEIwUk",
        "https://www.youtube.com/watch?v=DQF8hkN-wq8",
        "https://www.youtube.com/watch?v=7SuHtXxABb8",
        "https://www.youtube.com/watch?v=PyI0G_b3tNg",
        "https://www.youtube.com/watch?v=ADRWnp421HE",
        "https://www.youtube.com/watch?v=jW1_xrVCOf4",
        "https://www.youtube.com/watch?v=QiOBXXEWEhE",
        "https://www.youtube.com/watch?v=6FLNmQ-IHl8",
        "https://www.youtube.com/watch?v=_3ABNGbKuQ4",
        "https://www.youtube.com/watch?v=AQhqAhXHF4Y",
        "https://www.youtube.com/watch?v=wT_e5TpvSd8",
        "https://www.youtube.com/watch?v=4Xge42hk8vw",
        "https://www.youtube.com/watch?v=xY_PC4G9jBY",
        "https://www.youtube.com/watch?v=CoUDK9te-vk",
        "https://www.youtube.com/watch?v=XVU1SQI5aw0",
        "https://www.youtube.com/watch?v=WV6mMT7Ffb8",
        "https://www.youtube.com/watch?v=Hhy6MS_w1dg",
        "https://www.youtube.com/watch?v=pUx5d-019A8",
        "https://www.youtube.com/watch?v=Kt7jcOsdLmQ",
        "https://www.youtube.com/watch?v=Ag7WSF8UPF0",
        "https://www.youtube.com/watch?v=TGQQVvMWwJA",
        "https://www.youtube.com/watch?v=Dski1pc-ZV8",
        "https://www.youtube.com/watch?v=Ebq9VnivoYc",
        "https://www.youtube.com/watch?v=GjN5F85KAlQ",
        "https://www.youtube.com/watch?v=C0WVXLM0qzY",
        "https://www.youtube.com/watch?v=CFYWHT69YvQ",
        "https://www.youtube.com/watch?v=AlQ9oI06aSg",
        "https://www.youtube.com/watch?v=nH0YBC49YiU",
        "https://www.youtube.com/watch?v=CLqQz2k2HrA",
        "https://www.youtube.com/watch?v=V_8VENvWt0A",
        "https://www.youtube.com/watch?v=-bvb-AGTL1A",
        "https://www.youtube.com/watch?v=mB-HL8jYD2A",
        "https://www.youtube.com/watch?v=K57fyG1kJ54",
        "https://www.youtube.com/watch?v=wyS3Md2GJys",
        "https://www.youtube.com/watch?v=QBybRCoaaSA",
        "https://www.youtube.com/watch?v=gXrs1DGFXmI",
        "https://www.youtube.com/watch?v=XyfERoSf-G4",
        "https://www.youtube.com/watch?v=c-WzNyh43pI",
        "https://www.youtube.com/watch?v=G_tToSjCkJk",
        "https://www.youtube.com/watch?v=0DECXgCR8nI",
        "https://www.youtube.com/watch?v=2uO61U-gosc",
        "https://www.youtube.com/watch?v=tLs432xN5AA",
        "https://www.youtube.com/watch?v=LfQESle0MNU",
        "https://www.youtube.com/watch?v=FJlvr9Oqk8g",
        "https://www.youtube.com/watch?v=Cado0OrMhTs",
        "https://www.youtube.com/watch?v=wUtl7kVP16c",
        "https://www.youtube.com/watch?v=B79bdjig1Mo",
        "https://www.youtube.com/watch?v=_N4yUJLIcjM",
        "https://www.youtube.com/watch?v=1Pc65D35vKc"
    ]

    with ThreadPoolExecutor() as ex:
        futures = [ex.submit(scrape_video, url) for url in colossal_yt_links]
    scrape_video(colossal_yt_links[0])

    df = pd.read_csv(youtube_data_csv)