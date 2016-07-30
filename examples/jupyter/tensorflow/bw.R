library(ggplot2)

bw <- read.csv("~/workspace-saga/bigjob/Pilot-InMemory/jupyter/benchmark/bw.csv")

levels(bw$Scenario)[levels(bw$Scenario)=="ComputeDistanceNumpy"] <- "Numpy"
levels(bw$Scenario)[levels(bw$Scenario)=="ComputeDistanceTensorflow"] <- "Tensorflow"

p <- ggplot(bw, aes(x=Points, y=Time, colour=Scenario)) + 
  geom_line(stat="summary", fun.y="mean", size=1.2) +  geom_point(stat="summary", fun.y="mean", size=2) +
  stat_summary(fun.data = "mean_sdl", geom = "errorbar", width=.2, position = position_dodge(width = 0.90)) +
  scale_fill_brewer(palette="Paired")+ scale_colour_brewer(palette="Paired")+
  theme_bw(base_size = 32) + theme(legend.position = "bottom", legend.title=element_blank()) + 
  #scale_x_continuous(trans=log2_trans())+   
  xlab("Number of Points") + 
  ylab("Time (in sec)\n") +
  guides(fill=guide_legend(reverse=FALSE, title=element_blank(), ncol=5, keywidth=1))

p

pdf("alexnet.pdf", width=14, height=9)
p
dev.off()
