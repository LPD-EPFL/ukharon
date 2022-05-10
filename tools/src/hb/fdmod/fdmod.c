#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <rdma/ib_verbs.h>

#include <linux/inet.h>
#include <linux/string.h>

#include <stdbool.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("EPFL DCL");
MODULE_DESCRIPTION("RDMA FD multicast");

// different ib_verbs.h
#if true
#define ADD_DEVICE_RETURN_TYPE int
#define ADD_DEVICE_RETURN(a) a
#else
#define ADD_DEVICE_RETURN_TYPE void
#define ADD_DEVICE_RETURN(a)
#endif

// technical
#define ATTACH_MCAST true

// config
#define CQ_DEPTH 1
#define HCA_PORT 1
static char *DEVICE_NAME = "mlx5_1";
#define MC_QPN 0xFFFFFF
#define QKEY 0
#define PSN 0
static char *IPV6_GID = "ff12:401b:ffff::1";
static int LID = 0xC003;

// parameters
module_param(DEVICE_NAME, charp, 0000);
module_param(IPV6_GID, charp, 0000);
module_param(LID, int, 0000);

static struct ib_pd *pd = NULL;
static struct ib_cq *recv_cq = NULL;
static struct ib_cq *send_cq = NULL;
static struct ib_qp *qp = NULL;
static struct ib_ah *ah = NULL;
static union ib_gid gid;
static bool mc_attached = false;
static struct ib_device *in_use_device = NULL;

int heartbeat_send_multicast(int);

static void recv_completion_handler(struct ib_cq *cq, void *ctx) {
  struct ib_wc wc;
  ib_poll_cq(cq, 1, &wc);
  pr_info("Recv CH, WC status: %d\n", wc.status);
}

static void send_completion_handler(struct ib_cq *cq, void *ctx) {
  struct ib_wc wc;
  ib_poll_cq(cq, 1, &wc);
  pr_info("Send CH, WC status: %d\n", wc.status);
}

static void qp_event_handler(struct ib_event *event, void *ctx) {
  pr_alert("Unexpected QP event: %s\n", ib_event_msg(event->event));
}

static ADD_DEVICE_RETURN_TYPE add_device(struct ib_device *device) {
  int ret;

  pr_info("IB device #%d detected: %s.\n", device->index, device->name);
  if (strcmp(device->name, DEVICE_NAME) != 0) {
    pr_info("IB %s device isn't %s, ignoring.\n", device->name, DEVICE_NAME);
    return ADD_DEVICE_RETURN(0);
  }
  in_use_device = device;

  struct ib_port_attr port_attr;
  ret = ib_query_port(device, HCA_PORT, &port_attr);
  if (ret) {
    pr_alert("Failed to query port %d: %d\n", HCA_PORT, ret);
    return ADD_DEVICE_RETURN(ret);
  }

  pd = ib_alloc_pd(device, IB_ACCESS_LOCAL_WRITE | IB_ACCESS_REMOTE_READ |
                               IB_ACCESS_REMOTE_WRITE);

  if (IS_ERR(pd)) {
    pr_alert("Failed to allocate the PD.\n");
    pr_err("%s:%d error code for the PD %ld\n", __func__, __LINE__,
           PTR_ERR(pd));
    return ADD_DEVICE_RETURN(PTR_ERR(pd));
  }
  pr_info("Successfully allocated the PD in %p\n", pd);

  struct ib_cq_init_attr cq_init_attr = {.cqe = CQ_DEPTH};
  recv_cq = ib_create_cq(device, recv_completion_handler, qp_event_handler,
                         NULL, &cq_init_attr);
  if (IS_ERR(recv_cq)) {
    pr_alert("Error creating recv CQ.\n");
    pr_err("%s:%d error code for recv CQ %ld\n", __func__, __LINE__,
           PTR_ERR(recv_cq));
    return ADD_DEVICE_RETURN(PTR_ERR(recv_cq));
  }
  pr_info("Recv CQ successfully created in %p\n", recv_cq);

  send_cq = ib_create_cq(device, send_completion_handler, qp_event_handler,
                         NULL, &cq_init_attr);
  if (IS_ERR(send_cq)) {
    pr_alert("Error creating send CQ.\n");
    pr_err("%s:%d error code for send cq %ld\n", __func__, __LINE__,
           PTR_ERR(send_cq));
    return ADD_DEVICE_RETURN(PTR_ERR(send_cq));
  }
  pr_info("Send CQ successfully created in %p\n", send_cq);

  struct ib_qp_init_attr qp_init_attr = {
      .event_handler = qp_event_handler,
      .cap =
          {
              .max_send_wr = 2,
              .max_recv_wr = 2,
              .max_recv_sge = 1,
              .max_send_sge = 1,
          },
      .sq_sig_type = IB_SIGNAL_ALL_WR,
      .qp_type = IB_QPT_UD,
      .send_cq = send_cq,
      .recv_cq = recv_cq,
  };
  qp = ib_create_qp(pd, &qp_init_attr);
  if (IS_ERR(qp)) {
    pr_alert("Error creating QP\n");
    pr_err("%s:%d error code %ld\n", __func__, __LINE__, PTR_ERR(qp));
    return ADD_DEVICE_RETURN(PTR_ERR(qp));
  }

  pr_info("The QP was successfully created in %p\n", qp);

  /* Move the UD QP into the INIT state */
  struct ib_qp_attr qps_init_attr = {
      .qp_state = IB_QPS_INIT,
      .pkey_index = 0,
      .port_num = HCA_PORT,
      .qkey = QKEY,
  };
  ret = ib_modify_qp(qp, &qps_init_attr,
                     IB_QP_STATE | IB_QP_PKEY_INDEX | IB_QP_PORT | IB_QP_QKEY);
  if (ret) {
    pr_alert("Error moving QP to INIT\n");
    pr_err("%s:%d error code %d\n", __func__, __LINE__, ret);
    return ADD_DEVICE_RETURN(ret);
  }

  /* Move the QP to RTR */
  struct ib_qp_attr qps_rtr_attr = {
      .qp_state = IB_QPS_RTR,
  };
  ret = ib_modify_qp(qp, &qps_rtr_attr, IB_QP_STATE);
  if (ret) {
    pr_alert("Error moving QP to RTR\n");
    pr_err("%s:%d error code %d\n", __func__, __LINE__, ret);
    return ADD_DEVICE_RETURN(ret);
  }

  /* Move the QP to RTS */
  struct ib_qp_attr qps_rts_attr = {
      .qp_state = IB_QPS_RTS,
      .sq_psn = PSN,
  };
  ret = ib_modify_qp(qp, &qps_rts_attr, IB_QP_STATE | IB_QP_SQ_PSN);
  if (ret) {
    pr_alert("Error moving QP to RTS\n");
    pr_err("%s:%d error code %d\n", __func__, __LINE__, ret);
    return ADD_DEVICE_RETURN(ret);
  }

  pr_info("The QP was successfully transitioned to RTS.\n");

  ret = in6_pton(IPV6_GID, -1, (u8 *)(&gid.raw), -1, NULL);
  if (!ret) {
    pr_err("Couldn't pton IPV6_GID: %d\n", ret);
    return ADD_DEVICE_RETURN(ret);
  }

  struct rdma_ah_attr ah_attr = {
      .sl = 0,
      // .static_rate ?
      .port_num = HCA_PORT,
      .ah_flags = IB_AH_GRH,
      .type = RDMA_AH_ATTR_TYPE_IB,
      .ib =
          {
              .dlid = LID,
              .src_path_bits = 0,
          },
  };
  rdma_ah_set_dgid_raw(&ah_attr, &gid);

  ah = rdma_create_ah(pd, &ah_attr, 0);

  if (IS_ERR(ah)) {
    pr_alert("Error creating AH\n");
    pr_err("%s:%d error code %ld\n", __func__, __LINE__, PTR_ERR(ah));
    return ADD_DEVICE_RETURN(PTR_ERR(ah));
  }

  pr_info("The AH was successfully created in %p\n", ah);

#if ATTACH_MCAST
  ret = ib_attach_mcast(qp, &gid, LID);
  if (ret) {
    pr_err("Couldn't attach mcast group: %d\n", ret);
    return ADD_DEVICE_RETURN(ret);
  }
  mc_attached = true;
#endif

  pr_info("MC send WR intitiated...\n");
  return ADD_DEVICE_RETURN(0);
}

int heartbeat_send_multicast(int imm_data) {
  int ret;

  struct ib_ud_wr wr = {
      .wr =
          {
              .wr_id = 0,
              .sg_list = NULL,
              .num_sge = 0,
              .opcode = IB_WR_SEND_WITH_IMM,
              .ex.imm_data = imm_data,
              .send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE,
          },
      // header, hlen, mss,
      .ah = ah,
      .remote_qpn = MC_QPN,
      .remote_qkey = QKEY,
      .pkey_index = 0,
      .port_num = HCA_PORT,
  };

  const struct ib_send_wr *bad_wr = NULL;
  ib_req_notify_cq(send_cq, IB_CQ_NEXT_COMP);
  ret = ib_post_send(qp, (struct ib_send_wr *)&wr, &bad_wr);
  if (ret) {
    pr_err("IB post send error: %d\n", ret);
    return ret;
  }
  ib_req_notify_cq(send_cq, IB_CQ_NEXT_COMP);

  if (bad_wr) {
    pr_err("Got bad wr with id: %lld\n", bad_wr->wr_id);
    return 1;
  }

  return 0;
}
EXPORT_SYMBOL(heartbeat_send_multicast);

static void cleanup(void) {
  if (mc_attached) {
    ib_detach_mcast(qp, &gid, LID);
    mc_attached = false;
  }

  if (ah) {
    int ret = rdma_destroy_ah(ah, 0);
    ah = NULL;
    if (ret) pr_err("Error destroying AH: %d\n", ret);
  }

  if (qp) {
    int ret = ib_destroy_qp(qp);
    qp = NULL;
    if (ret) pr_err("Error destroying QP: %d\n", ret);
  }

  if (recv_cq) {
    ib_destroy_cq(recv_cq);
    recv_cq = NULL;
  }

  if (send_cq) {
    ib_destroy_cq(send_cq);
    send_cq = NULL;
  }

  if (pd) {
    ib_dealloc_pd(pd);
    pd = NULL;
  }
}

static void remove_device(struct ib_device *device, void *ctx) {
  if (device == in_use_device) {
    cleanup();
    in_use_device = NULL;
  }
  pr_info("IB device #%d %s removed.\n", device->index, device->name);
}

static struct ib_client client = {
    .name = "fdRdmaClient",
    .add = add_device,
    .remove = remove_device,
};

static int fdmod_init(void) {
  pr_info("Initing the FD module.\n");
  pr_info("DEVICE_NAME %s\n", DEVICE_NAME);
  pr_info("IPV6_GID = %s\n", IPV6_GID);
  pr_info("LID %d\n", LID);
  int ret = ib_register_client(&client);
  if (ret) {
    pr_alert("Failed to register the FD module.\n");
    return ret;
  }
  pr_info("Successfully registered the FD module.\n");
  return 0;
}

static void fdmod_exit(void) {
  cleanup();
  pr_info("FD module cleaned up.\n");
}

module_init(fdmod_init);
module_exit(fdmod_exit);
