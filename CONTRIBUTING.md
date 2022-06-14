# Workflow

For each task there will be an issue in GitLab assigned to a person with the label ~"To Do". As soon as the assigned person starts the task, he must remove the ~"To Do" label and add the ~"Doing" label. After completing the task, remove the ~"Doing" label and add the ~"Review" label.

We have implemented GitLab Flow - Environment Branches. If you don't know what I'm talking about or you have doubts, I invite you to watch the videos that you find in the list below:

- https://www.youtube.com/watch?v=ZJuUz5jWb44
- https://www.youtube.com/watch?v=K5Ux_m0ccuo
- https://www.youtube.com/watch?v=xfFwQiu86O8

Basically, **for each issue we must __Create a merge request and branch__.** After all changes and commits are done, remove **Draft: ** from the merge request title.

In the **Reviewer** field you must choose **Luis**.

A little tip: `git pull --prune` « use this command to remove from you local repository all deleted branches.
